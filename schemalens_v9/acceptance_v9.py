"""V9 acceptance: gates recomputed from raw data and from a fresh regeneration of the method; tamper tests.

global_pass_hardcoded = False: the final status is the conjunction of the gates computed below.
Negative tests tamper with in-memory COPIES of the evidence and require the corresponding gate to reject it.
"""
from __future__ import annotations

import copy
import gzip
import json

from . import compose as K
from . import phase2 as F
from . import phase6 as B
from .common import RUN, now_iso, read_json, sha256_obj, write_json
from .phase2_stages import generate

GLOBAL_PASS_HARDCODED = False
W = B.WORKLOADS


# ------------------------------------------------------------------------------------------- evidence
def load_bundle() -> dict:
    H = read_json(RUN / "stages/E15_composition/H.json")
    ev, act, P, C, _ = generate()
    return {"H": H, "frozen": read_json(RUN / "stages/E16_hbench_frozen/HBENCH_FROZEN.json"), "act": act, "P": P,
            "C": {t["canonical_id"]: t for t in C}, "mech": read_json(RUN / "stages/E13_route_synthesis/MECHANISM_USAGE.json"),
            "e18": read_json(RUN / "stages/E18_correctness/CORRECTNESS.json"), "e19": read_json(RUN / "stages/E19_baselines/BASELINES.json"),
            "e20": {d: read_json(B.OUT / f"{d}_RESULT.json") for d in B.order()},
            "e22": read_json(RUN / "stages/E22_criteria_ablations/ABLATIONS_PHYSICAL.json", None),
            "e23": read_json(RUN / "stages/E23_full_scale_acceptance/FULL_COLD_COMPARISON.json", None)}


def raw_rows(e20: dict):
    for d, r in e20.items():
        for f in r["raw_files"]:
            with gzip.open(f["path"], "rt") as fh:
                for line in fh:
                    yield d, json.loads(line)


# ------------------------------------------------------------------------------------------- gates
def g_generator_hash(b) -> dict:
    """Each schema of Hbench must equal the generator's output (a hand edit changes the DDL hash)."""
    fresh = K.compose()
    fresh_ddl = {s["schema_id"]: sha256_obj(s["ddl"]) for s in fresh["H"]}
    bad = [m["schema_id"] for m in b["frozen"]["Hbench"] if fresh_ddl.get(m["schema_id"]) != m["ddl_sha256"]]
    return {"status": "PASS" if not bad else "FAIL", "mismatched": bad}


def g_supersedes(b) -> dict:
    bad = []
    for s in b["H"]["H"]:
        tabs = set(s["canonical_ids"])
        for tid in tabs:
            for sup in b["C"][tid]["supersedes"]:
                for u in tabs - {tid}:
                    if any(o["family"] == sup["family"] and o["statement"] == sup["statement"] for o in b["C"][u]["origin_all"]):
                        bad.append({"schema": s["schema_id"], "table": tid, "keeps_superseded": u})
    return {"status": "PASS" if not bad else "FAIL", "violations": bad}


def g_activation(b) -> dict:
    """Every specification's family must have its predicate true for its statement in the mixes it claims."""
    bad = []
    for p in b["P"]:
        o = p["origin"]
        stmts = o["statement"] if isinstance(o["statement"], list) else [o["statement"]]
        for m in o["mixes"]:
            for s in stmts:
                if o["family"] not in b["act"][m][s]["active"]:
                    bad.append({"spec": p["spec_id"], "statement": s, "family": o["family"], "mix": m})
    return {"status": "PASS" if not bad else "FAIL", "violations": bad[:20]}


def g_mechanisms(b) -> dict:
    """The declared effect of each mechanism (physical C, H) must equal the effect recomputed by neutralising it.
    Both directions: declared-used without effect, and declared-unused with effect, are rejected (D-V9-014)."""
    bad, levels = [], {}
    base = sha256_obj(sorted(t["physical_id"] for t in b["C"].values()))
    base_H = sha256_obj(sorted(tuple(s["physical_ids"]) for s in K.compose()["H"]))
    for k, rec in b["mech"].items():
        _, _, _, Ck, _ = generate({k: False})
        phys = sha256_obj(sorted(t["physical_id"] for t in Ck)) != base
        hch = sha256_obj(sorted(tuple(s["physical_ids"]) for s in K.compose({k: False})["H"])) != base_H
        if bool(rec.get("C_physical_changes", rec.get("output_changes"))) != phys or ("H_changes" in rec and bool(rec["H_changes"]) != hch) \
                or bool(rec.get("output_changes")) != phys:
            bad.append(k)
        levels[k] = rec.get("level")
    return {"status": "PASS" if not bad else "FAIL", "declaration_mismatch": bad, "levels": levels,
            "no_physical_effect": [k for k, r in b["mech"].items() if not r.get("C_physical_changes", r.get("output_changes")) and not r.get("H_changes")]}


def g_correctness(b) -> dict:
    rows = {k: (v["agree_oracle"], v["evaluations"]) for k, v in b["e18"].items()}
    rows.update({k: (v["agree_oracle"], v["evaluations"]) for k, v in b["e19"]["correctness"].items()})
    bad = [k for k, (a, n) in rows.items() if a != n or n == 0]
    return {"status": "PASS" if not bad else "FAIL", "per_schema": {k: f"{a}/{n}" for k, (a, n) in rows.items()}, "failed": bad}


def g_cohort_benchmark(b, rows_iter=None) -> dict:
    """Recount the raw observations, recompute agreement with the V8 reference, check protocol, errors, isolation, restoration."""
    ref = B.reference_hashes()
    expected_per_dep = len(W) * B.REPS * B.MEASURED
    n_req, n_err, agree, total = {}, 0, 0, 0
    for d, r in (rows_iter or raw_rows(b["e20"])):
        n_req[d] = n_req.get(d, 0) + 1
        n_err += r["error"] is not None
        for st in r["statements"]:
            total += 1
            agree += ref.get((r["workload"], r["repetition"], r["logical_request_id"], st["statement_id"])) == st["canonical_hash"]
    proto = all(v["protocol"]["repetitions"] == B.REPS and v["protocol"]["measured"] == B.MEASURED and v["protocol"]["warmup"] == B.WARMUP for v in b["e20"].values())
    iso = all(v["isolation"]["clean"] for v in b["e20"].values())
    restored = all(not v["state_restoration"]["tables_changed"] for v in b["e20"].values())
    counts_ok = all(n_req.get(d) == expected_per_dep for d in b["e20"])
    ok = counts_ok and n_err == 0 and agree == total and total > 0 and proto and iso and restored
    return {"status": "PASS" if ok else "FAIL", "requests": n_req, "errors": n_err, "agreement": f"{agree}/{total}",
            "protocol_frozen": proto, "isolation_clean": iso, "state_restored": restored}


def g_ablations(b) -> dict:
    if not b["e22"]:
        return {"status": "NOT_EXECUTED"}
    ok = all(v == "PASS" for v in b["e22"]["agreement"].values()) and not any(b["e22"]["errors"].values())
    return {"status": "PASS" if ok else "FAIL", "agreement": b["e22"]["agreement"]}


def g_full_scale(b) -> dict:
    if not b["e23"]:
        return {"status": "NOT_EXECUTED"}
    ok = all(v == "PASS" for v in b["e23"]["agreement"].values()) and b["e23"]["cache_control"]["all_ok"]
    return {"status": "PASS" if ok else "FAIL", "agreement": b["e23"]["agreement"], "cache_control": b["e23"]["cache_control"]}


GATES = {"generator_hash": g_generator_hash, "supersedes": g_supersedes, "activation": g_activation, "mechanisms": g_mechanisms,
         "correctness": g_correctness, "cohort_benchmark": g_cohort_benchmark, "criteria_ablations": g_ablations, "full_scale": g_full_scale}


# ------------------------------------------------------------------------------------------- tamper tests
def negative_tests(b) -> list[dict]:
    tests = []

    def expect_fail(name, gate, bb, **kw):
        r = GATES[gate](bb, **kw) if kw else GATES[gate](bb)
        tests.append({"test": name, "gate": gate, "rejected": r["status"] == "FAIL", "passed": r["status"] == "FAIL"})

    # V9-specific
    bb = copy.deepcopy(b)
    # first (schema, table) whose superseded counterpart actually exists in C (some superseded specs never reach C)
    pick = next(((s, u) for s in bb["H"]["H"] for tid in s["canonical_ids"] for sup in bb["C"][tid]["supersedes"]
                 for u, t in bb["C"].items()
                 if u != tid and any(o["family"] == sup["family"] and o["statement"] == sup["statement"] for o in t["origin_all"])), None)
    if pick:
        s0, target = pick
        s0["canonical_ids"] = sorted(set(s0["canonical_ids"]) | {target})
    expect_fail("esquema mantém tabela marcada como substituída", "supersedes", bb)
    bb = copy.deepcopy(b)
    p = next(x for x in bb["P"] if x["origin"]["family"] == "C1")
    p["origin"]["family"] = "C7"
    expect_fail("família ativada sem predicado verdadeiro", "activation", bb)
    bb = copy.deepcopy(b)
    bb["frozen"]["Hbench"][0]["ddl_sha256"] = "0" * 64
    expect_fail("esquema com hash diferente da saída do gerador (edição manual)", "generator_hash", bb)
    bb = copy.deepcopy(b)
    k0 = next(k for k, r in bb["mech"].items() if not r["output_changes"])
    bb["mech"][k0] = dict(bb["mech"][k0], output_changes=True, C_physical_changes=True)
    expect_fail("mecanismo declarado usado cujo desligamento não muda a saída", "mechanisms", bb)
    # inherited from V8-012 (applied to the V9 evidence)
    first = [True]

    def tampered(kind):
        for d, r in raw_rows(b["e20"]):
            if first[0]:
                first[0] = False
                if kind == "hash":
                    r["statements"][0]["canonical_hash"] = "x"
                elif kind == "drop":
                    continue
                elif kind == "error":
                    r["error"] = "Injected"
            yield d, r
    for kind, name in (("hash", "hash de resposta alterado"), ("drop", "observação apagada"), ("error", "erro injetado")):
        first[0] = True
        expect_fail(name, "cohort_benchmark", b, rows_iter=tampered(kind))
    for name, mut in (("carga concorrente", lambda v: v["isolation"].update(clean=False)),
                      ("estado não restaurado", lambda v: v["state_restoration"].update(tables_changed=["x"])),
                      ("protocolo reduzido", lambda v: v["protocol"].update(repetitions=4))):
        bb = copy.deepcopy(b)
        mut(next(iter(bb["e20"].values())))
        expect_fail(name, "cohort_benchmark", bb)
    bb = copy.deepcopy(b)
    k = next(iter(bb["e18"]))
    bb["e18"][k]["agree_oracle"] -= 1
    expect_fail("divergência do oráculo escondida", "correctness", bb)
    return tests


def run() -> dict:
    b = load_bundle()
    gates = {k: f(b) for k, f in GATES.items()}
    neg = negative_tests(b)
    executed = {k: v for k, v in gates.items() if v["status"] != "NOT_EXECUTED"}
    status = "PASS" if all(v["status"] == "PASS" for v in executed.values()) and all(t["passed"] for t in neg) else "FAIL"
    out = {"run_id": "v9-generation-20261112-001", "evaluated_at": now_iso(), "global_pass_hardcoded": GLOBAL_PASS_HARDCODED,
           "acceptance_status": status, "gates": gates, "negative_tests": neg,
           "not_executed": [k for k, v in gates.items() if v["status"] == "NOT_EXECUTED"]}
    write_json(RUN / "ACCEPTANCE_V9.json", out)
    return out


if __name__ == "__main__":
    r = run()
    print(json.dumps({"acceptance_status": r["acceptance_status"], "gates": {k: v["status"] for k, v in r["gates"].items()},
                      "negative_tests": f"{sum(t['passed'] for t in r['negative_tests'])}/{len(r['negative_tests'])}"}, ensure_ascii=False))
