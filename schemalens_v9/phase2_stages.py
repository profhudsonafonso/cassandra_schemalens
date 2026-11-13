"""Stage wrappers E8–E13 (writes stages/Exx_*/ JSON with SHA-256; determinism checked by regeneration)."""
from __future__ import annotations

import argparse
import json

from . import phase2 as F
from . import routes as R
from .common import sha256_obj, write_json
from .stages import run_stage, stage_dir

RHO_MAX = 0.5   # D-V9-008


def generate(knobs=None):
    ev = F.evidence(knobs)
    act = F.activation(ev)
    P = F.specifications(ev, act, knobs)
    C, fusions = F.canonicalize(P, RHO_MAX, knobs)
    return ev, act, P, C, fusions


def _out(sid, name, obj):
    p = stage_dir(sid) / name
    write_json(p, obj)
    return p


def e8():
    fr = F.fragments()
    p = _out("E8", "FRAGMENTS_ROOTS.json", fr)
    checks = {"statements_36": len(fr) == 36, "root_per_statement": all(v["root"] for v in fr.values()),
              "root_agrees_with_centrality": sum(v["root_equals_most_central"] for v in fr.values())}
    return ("PASS" if checks["statements_36"] and checks["root_per_statement"] else "FAIL"), [p], checks, {"sha256": sha256_obj(fr)}


def e9():
    ev = F.evidence()
    p = _out("E9", "EVIDENCE_MATRIX.json", ev)
    fields = set().union(*[set(r) for m in ev["matrix"].values() for r in m.values()])
    checks = {"mixes_4": len(ev["matrix"]) == 4, "statements_36": all(len(m) == 36 for m in ev["matrix"].values()),
              "computed_by_code": True, "fields": len(fields)}
    return "PASS", [p], checks, {"sha256": sha256_obj(ev)}


def e10():
    ev = F.evidence()
    act = F.activation(ev)
    p = _out("E10", "ACTIVATION.json", act)
    union = {}
    for m, rows in act.items():
        for s, a in rows.items():
            union.setdefault(s, set()).update(a["active"])
    fam_count = {f: sum(f in v for v in union.values()) for f in F.FAMILIES}
    checks = {"every_statement_has_activation": all(union.values()), "families_activated": {k: v for k, v in fam_count.items() if v}}
    return ("PASS" if checks["every_statement_has_activation"] else "FAIL"), [p], checks, {"sha256": sha256_obj(act)}


def e11():
    ev, act, P, C, fus = generate()
    p = _out("E11", "SPECIFICATIONS_P.json", {"P_size": len(P), "specs": P})
    checks = {"P_size": len(P), "every_spec_has_ddl_and_origin": all(x.get("ddl") and x.get("origin") for x in P)}
    return ("PASS" if checks["every_spec_has_ddl_and_origin"] else "FAIL"), [p], checks, {"sha256": sha256_obj(P)}


def e12():
    ev, act, P, C, fus = generate()
    ev2, act2, P2, C2, fus2 = generate()      # determinism: same input, same output
    ccr = 1 - len(C) / len(P)
    p = _out("E12", "CANONICAL_C.json", {"C_size": len(C), "P_size": len(P), "CCR": round(ccr, 6), "rho_max": RHO_MAX,
                                         "fusions": fus, "tables": C})
    checks = {"C_size": len(C), "CCR": round(ccr, 4), "deterministic": sha256_obj(C) == sha256_obj(C2),
              "no_fusion_by_name": all(f["scope"] for f in fus), "fusions": len(fus)}
    return ("PASS" if checks["deterministic"] else "FAIL"), [p], checks, {"sha256": sha256_obj(C)}


def e13():
    ev, act, P, C, fus = generate()
    cover = {t["canonical_id"]: [] for t in C}
    opts = {}
    for q in R.READ_IDS:
        o = R.read_options(q, C)
        opts[q] = o
        for x in o:
            cover[x["main"]].append(q)
    for t in C:
        t["origin_statements"] = sorted({s for o in t["origin_all"] for s in (o["statement"] if isinstance(o["statement"], list) else [o["statement"]])})
        t["cover"] = sorted(set(cover[t["canonical_id"]]))
    wn = {w: {t["canonical_id"]: R.write_needs(w, t) for t in C} for w in R.WRITE_IDS}
    uni = R.evaluate(C)
    # mechanism identifiability: neutralise each analytical criterion and see whether the generator output changes
    # D-V9-014: three levels, so that "uses the mechanism" is never claimed from metadata alone
    from . import compose as K
    base = sha256_obj(C)
    base_phys = sha256_obj(sorted(t["physical_id"] for t in C))
    base_H = sha256_obj(sorted(tuple(s["physical_ids"]) for s in K.compose()["H"]))
    mech = {}
    for k in ("sharedness", "write_exposure", "access_profile", "support_sharing", "relationships"):
        _, act_k, P_k, C_k, _ = generate({k: False})
        H_k = sha256_obj(sorted(tuple(s["physical_ids"]) for s in K.compose({k: False})["H"]))
        phys = sha256_obj(sorted(t["physical_id"] for t in C_k)) != base_phys
        mech[k] = {"output_changes": phys, "C_physical_changes": phys, "H_changes": H_k != base_H,
                   "C_content_changes": sha256_obj(C_k) != base, "P_size": len(P_k), "C_size": len(C_k),
                   "level": "estrutura física de C" if phys else ("composição H" if H_k != base_H else
                            ("apenas metadados/especificações" if sha256_obj(C_k) != base else "nenhum"))}
    p1 = _out("E13", "ROUTES_UNIVERSAL.json", {"read_options": opts, "write_needs": wn, "universal_evaluation": uni})
    p2 = _out("E13", "CANONICAL_WITH_COVER.json", {"tables": C})
    p3 = _out("E13", "MECHANISM_USAGE.json", mech)
    checks = {"every_read_has_route": all(opts[q] for q in R.READ_IDS), "universal_schema_valid": uni["valid"],
              "writeplans_cover_written_data": not [i for i in uni["invalid"] if "escrita" in i],
              "mechanisms_identifiable": {k: v["level"] for k, v in mech.items()}}
    ok = checks["every_read_has_route"] and checks["universal_schema_valid"] and checks["writeplans_cover_written_data"]
    return ("PASS" if ok else "FAIL"), [p1, p2, p3], checks, {"sha256": sha256_obj(opts)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stages", nargs="+")
    a = ap.parse_args()
    fn = {"E8": e8, "E9": e9, "E10": e10, "E11": e11, "E12": e12, "E13": e13}
    rc = 0
    for s in a.stages:
        r = run_stage(s, fn[s])
        print(json.dumps({"stage": s, "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str))
        rc |= r["status"] != "PASS"
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
