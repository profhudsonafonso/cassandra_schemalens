"""Stage runners E17, E18, E19."""
from __future__ import annotations

import argparse
import copy
import gzip
import json
from collections import Counter

from . import oracle_v9 as O
from . import phase5 as M
from .common import OLD_RUN_011, RUN, now_iso, read_json, sha256_obj, write_json
from .contract import CONTRACT
from .stages import run_stage, set_stage, stage_dir

WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
EXTERNAL = {"V8_AUTO_H_norm": ("rubis_v9_v8_auto_norm", "schemalens_norm"), "NoSE": ("rubis_v9_cohort_nose_bidding", "nose_bidding"),
            "QPG": ("rubis_v9_cohort_qpg_bidding", "qpg_bidding")}


def h_schemas():
    H = read_json(RUN / "stages/E15_composition/H.json")
    C = {t["canonical_id"]: t for t in read_json(RUN / "stages/E13_route_synthesis/CANONICAL_WITH_COVER.json")["tables"]}
    return H["H"], C


def ks_of(schema_id: str) -> str:
    return f"rubis_v9_{schema_id.lower()}"


def cohort():
    cl, s = M.session()
    try:
        raw, ids = M.load_raw_state(s)
    finally:
        cl.shutdown()
    return raw, ids, M.model_state(raw)


def e17():
    raw, ids, state = cohort()
    man = M.cohort_manifest(raw, ids)
    mp = stage_dir("E17") / "COHORT_MANIFEST.json"
    write_json(mp, man)
    H, C = h_schemas()
    cl, s = M.session()
    rec = {}
    try:
        for i, h in enumerate(H):
            set_stage("E17", progress={"text": f"esquema {i + 1} de {len(H)} ({h['schema_id']})", "done": i, "total": len(H)})
            tabs = [C[t] for t in h["canonical_ids"]]
            rec[h["schema_id"]] = {"keyspace": ks_of(h["schema_id"]), "tables": M.materialize_generated(s, ks_of(h["schema_id"]), tabs, state),
                                   "ddl_sha256": sha256_obj(h["ddl"])}
    finally:
        cl.shutdown()
    out = stage_dir("E17") / "MATERIALIZATION.json"
    write_json(out, rec)
    exp = {"items": 5906, "users": 5616, "bids": 5690}
    checks = {"cohort_counts_match_statement": {k: man["cohort_counts"][k] == v for k, v in exp.items()},
              "all_tables_fingerprint_match": all(t["match"] for r in rec.values() for t in r["tables"].values()),
              "keyspaces": [r["keyspace"] for r in rec.values()], "closure_users": len(man["closure_users"])}
    ok = all(checks["cohort_counts_match_statement"].values()) and checks["all_tables_fingerprint_match"]
    return ("PASS" if ok else "FAIL"), [mp, out], checks, {}


def v8_reference(rep=0):
    ref = {}
    for w in WORKLOADS:
        with gzip.open(OLD_RUN_011 / f"full_scale_matched_cohort/raw/AUTO_H_norm_{w}.jsonl.gz", "rt") as fh:
            for line in fh:
                r = json.loads(line)
                if r["repetition"] == rep:
                    ref[(w, r["logical_request_id"])] = {st["statement_id"]: st["logical_rows"] for st in r["statements"]}
    return ref


def replay(adapter, state0, ref, label, stage):
    from reused.methodology.oracle_011 import canonical
    from reused.methodology.workload_005 import histories
    from .phase1 import _benchmark_domain
    dom = _benchmark_domain()
    agree_o, agree_v8, total, mism = Counter(), Counter(), Counter(), []
    for wi, w in enumerate(WORKLOADS):
        set_stage(stage, progress={"text": f"{label}: carga {w} (repetição 0)", "done": wi, "total": 4})
        st = O.fresh(state0)
        adapter.reset_state()
        for phase, n in (("warmup", 1000), ("measured", 10000)):
            for req in histories(w, 0, n, dom, "v9", phase=phase):
                ctx_a, ctx_o = {}, {}
                for sid in req["constituent_statement_ids"]:
                    got = canonical(adapter.execute_statement(sid, req["parameters"], ctx_a)["logical_rows"])
                    exp = canonical(O.evaluate(st, sid, req["parameters"], ctx_o))
                    if phase != "measured":
                        continue
                    total[sid] += 1
                    agree_o[sid] += got == exp
                    agree_v8[sid] += got == ref.get((w, req["logical_request_id"]), {}).get(sid)
                    if got != exp and len(mism) < 20:
                        mism.append({"workload": w, "request": req["logical_request_id"], "sid": sid, "expected": exp, "got": got})
        adapter.reset_state()
    return {"evaluations": sum(total.values()), "agree_oracle": sum(agree_o.values()), "agree_v8_reference": sum(agree_v8.values()),
            "per_statement": {str(k): {"n": total[k], "oracle": agree_o[k], "v8": agree_v8[k]} for k in sorted(total)}, "first_mismatches": mism}


def restoration(s, ks, tables, expected):
    bad = []
    for t in tables:
        refs = t["partition"] + [r for r, _ in t["clustering"]] + t["columns"]
        if M.canon_rows(M.read_back(s, ks, t["name"], refs)) != expected[t["name"]]["expected_fingerprint"]:
            bad.append(t["name"])
    return bad


def e18():
    raw, ids, state = cohort()
    H, C = h_schemas()
    mat = read_json(stage_dir("E17") / "MATERIALIZATION.json")
    ref = v8_reference()
    cl, s = M.session()
    rec = {}
    try:
        for h in H:
            ks = ks_of(h["schema_id"])
            tabs = {t: C[t] for t in h["canonical_ids"]}
            ad = M.GeneratedAdapter(s, ks, h, tabs, state)
            r = replay(ad, state, ref, h["schema_id"], "E18")
            r["restoration_tables_changed"] = restoration(s, ks, list(tabs.values()), mat[h["schema_id"]]["tables"])
            rec[h["schema_id"]] = r
    finally:
        cl.shutdown()
    out = stage_dir("E18") / "CORRECTNESS.json"
    write_json(out, rec)
    checks = {k: {"oracle": f"{v['agree_oracle']}/{v['evaluations']}", "v8_ref": f"{v['agree_v8_reference']}/{v['evaluations']}",
                  "restored": not v["restoration_tables_changed"]} for k, v in rec.items()}
    ok = all(v["agree_oracle"] == v["evaluations"] and not v["restoration_tables_changed"] for v in rec.values())
    return ("PASS" if ok else "FAIL"), [out], checks, {"blocked_schemas": [k for k, v in rec.items() if v["agree_oracle"] != v["evaluations"]]}


def e19():
    from reused.methodology.semantic_replay_011 import DeploymentAdapter008, source_closure
    raw, ids, state = cohort()
    ref = v8_reference()
    cl, s = M.session()
    mat, cor = {}, {}
    try:
        set_stage("E19", progress={"text": "materializando SL norm da V8, NoSE e QPG", "done": 0, "total": 6})
        mat["V8_AUTO_H_norm"] = M.materialize_v8_norm(s, EXTERNAL["V8_AUTO_H_norm"][0], raw)
        mat["NoSE"] = M.materialize_baseline(s, EXTERNAL["NoSE"][0], "nose_bidding", raw)
        mat["QPG"] = M.materialize_baseline(s, EXTERNAL["QPG"][0], "qpg_bidding", raw)
        closure, _sid = source_closure(s)
        for name, (ks, kind) in EXTERNAL.items():
            ad = DeploymentAdapter008(s, name, ks, kind, closure)
            cor[name] = replay(ad, state, ref, name, "E19")
    finally:
        cl.shutdown()
    out = stage_dir("E19") / "BASELINES.json"
    write_json(out, {"materialization": mat, "correctness": cor,
                     "declared": "NoSE e QPG reconstruídos dos artefatos históricos (DDL/SQL), não execuções dos otimizadores"})
    checks = {k: {"oracle": f"{v['agree_oracle']}/{v['evaluations']}", "v8_ref": f"{v['agree_v8_reference']}/{v['evaluations']}"} for k, v in cor.items()}
    checks["materialized"] = {k: all(t.get("match", True) for t in (v["tables"].values())) for k, v in mat.items()}
    ok = all(v["agree_oracle"] == v["evaluations"] for v in cor.values()) and all(checks["materialized"].values())
    return ("PASS" if ok else "FAIL"), [out], checks, {}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("stages", nargs="+")
    a = ap.parse_args()
    for sid in a.stages:
        r = run_stage(sid, {"E17": e17, "E18": e18, "E19": e19}[sid])
        print(json.dumps({"stage": sid, "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str), flush=True)
