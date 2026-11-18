"""E23 runner (resumable): select 2 best H -> build 4 full keyspaces -> verify -> cold benchmark -> compare."""
from __future__ import annotations

import json
import statistics
import time

from . import isolation
from . import phase5 as M
from . import phase5_run as PR
from . import phase6 as B
from . import phase8 as F8
from .common import RUN, now_iso, read_json, write_json
from .phase6b import tci, verdict
from .stages import incident, load_state, run_stage, save_state, set_stage

OUT = F8.OUT
W = B.WORKLOADS
EXT = {"NoSE": ("rubis_v9_full_nose_bidding", "nose_bidding"), "QPG": ("rubis_v9_full_qpg_bidding", "qpg_bidding")}


def ks_full(sid):
    return f"rubis_v9_full_{sid.lower()}"


def select_best() -> list[str]:
    an = read_json(RUN / "stages/E21_analysis/ANALYSIS.json")
    hid = [d for d in an["order"] if d.startswith("H")]
    wins = {h: sum(an["internal_H"][w]["measured_order"][0] == h for w in W) for h in hid}
    meanrank = {h: statistics.fmean(an["internal_H"][w]["measured_order"].index(h) for w in W) for h in hid}
    best = sorted(hid, key=lambda h: (-wins[h], meanrank[h], h))[:2]
    write_json(OUT / "SELECTION.json", {"rule": "os 2 esquemas de H que mais vezes foram o melhor medido na coorte (E21, janela contínua); desempate pela posição média",
                                        "wins": wins, "mean_rank": meanrank, "selected": best})
    return best


def build_all(best):
    """Builds (resumable).  For generated keyspaces, the domain-partition fingerprint right after the build is
    stored; if it differs later (e.g. a run was killed before reset_state), the keyspace is rebuilt."""
    H, C = PR.h_schemas()
    cl, s = F8.session(900)
    try:
        for sid in best:
            h = next(x for x in H if x["schema_id"] == sid)
            tabs = {t: C[t] for t in h["canonical_ids"]}
            cpp = OUT / f"build_{sid}.json"
            cp = read_json(cpp, {}) or {}
            if cp.get("post_build_domain_fingerprint"):
                fp = F8.domain_fingerprint(F8.FullGeneratedAdapter(s, ks_full(sid), h, tabs))
                if fp != cp["post_build_domain_fingerprint"]:
                    incident(f"E23: {sid} completo com partições do domínio alteradas (execução interrompida?); reconstruindo")
                    cp["tables"] = {}
                    write_json(cpp, cp)
            F8.build_generated(s, ks_full(sid), list(tabs.values()), cpp, f"{sid} completo")
            cp = read_json(cpp)
            if not cp.get("post_build_domain_fingerprint") or not cp.get("fingerprint_after_tables") == sorted(cp["tables"]):
                cp["post_build_domain_fingerprint"] = F8.domain_fingerprint(F8.FullGeneratedAdapter(s, ks_full(sid), h, tabs))
                cp["fingerprint_after_tables"] = sorted(cp["tables"])
                write_json(cpp, cp)
        for name, (ks, which) in EXT.items():
            F8.build_baseline(s, ks, which, OUT / f"build_{name}.json", f"{name} completo")
    finally:
        cl.shutdown()


def adapters(s, best, closure):
    from methodology.semantic_replay_011 import DeploymentAdapter008
    H, C = PR.h_schemas()
    out = {}
    for sid in best:
        h = next(x for x in H if x["schema_id"] == sid)
        out[sid] = (F8.FullGeneratedAdapter(s, ks_full(sid), h, {t: C[t] for t in h["canonical_ids"]}), ks_full(sid))
    for name, (ks, kind) in EXT.items():
        out[name] = (DeploymentAdapter008(s, name, ks, kind, closure), ks)
    return out


def replay_full(ad, ref, label):
    from methodology.semantic_replay_011 import hash_value
    from methodology.workload_005 import histories
    from .phase1 import _benchmark_domain
    dom = _benchmark_domain()
    n = agree = 0
    for wi, w in enumerate(W):
        set_stage("E23", progress={"text": f"verificação em escala completa: {label}, carga {w} (repetição 0)", "done": wi, "total": 4})
        ad.reset_state()
        for phase, cnt in (("warmup", 1000), ("measured", 10000)):
            for req in histories(w, 0, cnt, dom, "v9", phase=phase):
                ctx = {}
                for sid in req["constituent_statement_ids"]:
                    rows = ad.execute_statement(sid, req["parameters"], ctx)["logical_rows"]
                    if phase == "measured":
                        n += 1
                        agree += ref.get((w, 0, req["logical_request_id"], sid)) == hash_value(rows)
        ad.reset_state()
    return {"evaluations": n, "agree_v8_full_reference": agree}


def verify(best):
    from methodology.semantic_replay_011 import source_closure
    ref = F8.full_reference()
    cl, s = F8.session(900)
    rec = {}
    try:
        closure, _ = source_closure(s)
        ads = adapters(s, best, closure)
        for name, (ad, ks) in ads.items():
            fp0 = F8.domain_fingerprint(ad) if isinstance(ad, F8.FullGeneratedAdapter) else None
            r = replay_full(ad, ref, name)
            if fp0 is not None:
                r["domain_state_restored"] = F8.domain_fingerprint(ad) == fp0
            rec[name] = r
    finally:
        cl.shutdown()
    write_json(OUT / "FULL_VERIFICATION.json", rec)
    return rec


def cold_bench(best):
    from methodology.semantic_replay_011 import source_closure
    ref = F8.full_reference()
    seq = [best[0], "NoSE", best[1], "QPG"]
    raw_dir = OUT / "raw"
    for attempt in range(1, 6):
        cl, s = F8.session(900, reconnect=True)
        results, invalid = {}, None
        try:
            closure, _ = source_closure(s)
            ads = adapters(s, best, closure)
            for i, dep in enumerate(seq):
                done = read_json(OUT / f"{dep}_COLD_RESULT.json", {})
                if done.get("status") == "PASS" and done.get("attempt") == attempt:
                    results[dep] = done
                    continue
                ad, ks = ads[dep]
                drops, calls = [], [0]
                orig = ad.reset_state

                def cold_reset(_o=orig, _d=drops, _c=calls):
                    _o()
                    if _c[0] % 2 == 1:     # before each repetition (and once at protocol end): W*R + 1
                        _d.append(F8.cold_start(s))
                    _c[0] += 1
                ad.reset_state = cold_reset
                fp0 = F8.domain_fingerprint(ad) if isinstance(ad, F8.FullGeneratedAdapter) else None
                started = now_iso()
                with isolation.BulkWriterWatch() as watch:
                    res = B.run_protocol(ad, dep, ks, i, len(seq), stage="E23", raw_dir=raw_dir)
                iso = watch.report()
                ad.reset_state = orig
                restored = (F8.domain_fingerprint(ad) == fp0) if fp0 is not None else None
                agr = B.agreement(res["raw_files"], {k: v for k, v in ref.items()})
                errors = sum(x["errors"] for x in res["summaries"])
                cold_ok = len(drops) == len(W) * B.REPS + 1 and all(d["ok"] for d in drops)
                status = "PASS" if errors == 0 and agr["status"] == "PASS" and cold_ok and restored is not False else "FAIL"
                if status == "PASS" and not iso["clean"]:
                    status = "INVALID_CONCURRENT_LOAD"
                rec = {"deployment_id": dep, "keyspace": ks, "attempt": attempt, "status": status, "started_at": started, "finished_at": now_iso(),
                       "isolation": iso, "cache_drops": drops, "cold_start_ok": cold_ok, "agreement_with_v8_full_runs": agr,
                       "domain_state_restored": restored, "errors": errors, "summaries": res["summaries"], "raw_files": res["raw_files"]}
                write_json(OUT / f"{dep}_COLD_RESULT.json", rec)
                results[dep] = rec
                if status == "INVALID_CONCURRENT_LOAD":
                    invalid = dep
                    break
                if status != "PASS":
                    raise RuntimeError(f"cold {dep}: {status} (agreement {agr['status']}, cold_ok {cold_ok}, restored {restored}, errors {errors})")
        finally:
            cl.shutdown()
        if invalid is None:
            return results, seq
        st = load_state()
        st.setdefault("invalidated_measurements", []).append({"deployment": f"E23:{invalid}", "at": now_iso()})
        save_state(st)
        incident(f"E23: medição fria de {invalid} invalidada; janela recomeçada")
    raise RuntimeError("E23: medições frias invalidadas repetidamente")


def compare(results, seq, best):
    rep = {d: {w: [x["mean_ms"] for x in sorted(results[d]["summaries"], key=lambda x: x["repetition"]) if x["workload"] == w] for w in W} for d in seq}
    per = {d: {w: {**tci(rep[d][w]), "p50_ms": statistics.fmean(x["p50_ms"] for x in results[d]["summaries"] if x["workload"] == w),
                   "reads_per_request": sum(x["physical_reads"] for x in results[d]["summaries"] if x["workload"] == w) / 50000,
                   "writes_per_request": sum(x["physical_writes"] for x in results[d]["summaries"] if x["workload"] == w) / 50000} for w in W} for d in seq}
    con = {}
    for h in best:
        for b in ("NoSE", "QPG"):
            for w in W:
                c = tci([a - x for a, x in zip(rep[h][w], rep[b][w])])
                con.setdefault(h, {}).setdefault(b, {})[w] = {"diff_ms": c, "relative": c["mean"] / per[b][w]["mean"], "verdict_95": verdict(c)}
    out = {"scope": "escala completa (2 M itens / 20 M lances), nó reiniciado e cache zerado antes de cada repetição, mesma janela, intercalado",
           "order": seq, "windows": {d: [results[d]["started_at"], results[d]["finished_at"]] for d in seq},
           "per_deployment": per, "paired_contrasts": con,
           "cache_control": {"cycles": sum(len(results[d]["cache_drops"]) for d in seq), "all_ok": all(results[d]["cold_start_ok"] for d in seq),
                             "max_kib_after": max(x["page_cache_kib_after"] for d in seq for x in results[d]["cache_drops"])},
           "agreement": {d: results[d]["agreement_with_v8_full_runs"]["status"] for d in seq}}
    write_json(OUT / "FULL_COLD_COMPARISON.json", out)
    return out


def e23():
    sel = read_json(OUT / "SELECTION.json", {}).get("selected") or select_best()
    build_all(sel)
    ver = read_json(OUT / "FULL_VERIFICATION.json", None) or verify(sel)
    bad = {k: v for k, v in ver.items() if v["agree_v8_full_reference"] != v["evaluations"] or v.get("domain_state_restored") is False}
    if bad:
        return "FAIL", [OUT / "FULL_VERIFICATION.json"], {"verification_failed": list(bad)}, {}
    results, seq = cold_bench(sel)
    comp = compare(results, seq, sel)
    checks = {"selected": sel, "verification": {k: f"{v['agree_v8_full_reference']}/{v['evaluations']}" for k, v in ver.items()},
              "agreement": comp["agreement"], "cache_control_ok": comp["cache_control"]["all_ok"]}
    return "PASS", [OUT / "FULL_COLD_COMPARISON.json"], checks, {}


if __name__ == "__main__":
    r = run_stage("E23", e23)
    print(json.dumps({"stage": "E23", "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str), flush=True)
