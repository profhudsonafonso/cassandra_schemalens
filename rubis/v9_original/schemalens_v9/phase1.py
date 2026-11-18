"""Fase 1: E3 (EER), E4 (estatísticas), E5 (carga), E6 (contrato + oráculo), E7 (catálogo)."""
from __future__ import annotations

import argparse
import ast
import gzip
import json
import statistics
from array import array
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from .common import OLD_RUN_011, RUN, SOURCE_KEYSPACE, V9, now_iso, read_json, sha256_file, write_json
from .nose_parse import MODEL_RB, WORKLOAD_RB, parse_model, parse_statement, parse_workload
from .stages import run_stage, set_stage, stage_dir

NOSE_COMMIT = "d275634d99069bfd88a6c3ac51020138bc130dd4"
WORKLOADS = ("browsing", "bidding", "update10x", "update100x")


def _session(timeout=120):
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    c = Cluster(["127.0.0.1"])
    s = c.connect()
    s.row_factory = dict_factory
    s.default_timeout = timeout
    return c, s


def _v8_input(path, role):
    prov = RUN / "provenance/V8_INPUTS.json"
    d = read_json(prov, {"files": {}}) or {"files": {}}
    d["files"].setdefault(str(path), {"sha256": sha256_file(path), "read_by": role})
    d["updated_at"] = now_iso()
    write_json(prov, d)


# ------------------------------------------------------------------------------------------- E3
def e3():
    model = parse_model()
    c, s = _session()
    try:
        cols = {}
        for e in model["entities"]:
            cols[e] = {r["column_name"]: {"type": r["type"], "kind": r["kind"]} for r in
                       s.execute("SELECT column_name, type, kind FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s", (SOURCE_KEYSPACE, e))}
    finally:
        c.shutdown()
    entities, problems = {}, []
    for e, v in model["entities"].items():
        mapping = {}
        for a in v["attributes"]:
            n = a["name"]
            if n in cols[e]:
                mapping[n] = n
            else:
                cands = [col for col in cols[e] if col.endswith("_" + n) and col not in ("creation_date", "start_date", "end_date")]
                if len(cands) == 1:
                    mapping[n] = cands[0]
                else:
                    problems.append(f"{e}.{n}: sem coluna correspondente em rubis_full ({cands})")
        entities[e] = {"attributes": v["attributes"], "key": ["id"], "count_optimizer_nose": v["count_optimizer"],
                       "source_table": f"{SOURCE_KEYSPACE}.{e}", "source_columns": mapping}
    rels = []
    for r in model["relationships"]:
        fk = f"{r['name']}_id"
        if fk not in cols[r["from"]]:
            problems.append(f"FK {r['from']}.{fk} ausente em rubis_full")
        entities[r["from"]]["source_columns"][fk] = fk
        rels.append({"name": r["name"], "inverse": r["inverse"], "from": r["from"], "to": r["to"], "fk_attribute": fk,
                     "cardinality": "N:1", "from_side": {"min": 1, "max": 1, "declared": "HasOne (NoSE): cada instância de origem tem exatamente um alvo"},
                     "to_side": {"min": 0, "max": "N", "declared": "lado inverso: zero ou mais"}, "optionality_verified_in": "E4"})
    for e in entities:
        mapped = set(entities[e]["source_columns"].values())
        entities[e]["unmapped_source_columns"] = sorted(set(cols[e]) - mapped)
    v8_eer = read_json(V9 / "reused/inputs/rubis_eer.json", {})
    v8_rels = {(r["from"], r["name"], r["to"]) for r in v8_eer.get("relationships", [])}
    out = stage_dir("E3") / "CONCEPTUAL_SCHEMA.json"
    rec = {"source_model": str(MODEL_RB), "source_model_sha256": sha256_file(MODEL_RB), "nose_commit": NOSE_COMMIT,
           "source_ddl": "system_schema.columns de rubis_full", "entities": entities, "relationships": rels, "problems": problems}
    write_json(out, rec)
    checks = {"entities_7": len(entities) == 7, "relationships_10": len(rels) == 10, "no_mapping_problems": not problems,
              "matches_v8_relationship_list": {(r["from"], r["name"], r["to"]) for r in rels} == v8_rels,
              "every_relationship_has_cardinality": all(r["cardinality"] for r in rels)}
    return ("PASS" if all(checks.values()) else "FAIL"), [out, MODEL_RB], checks, {}


# ------------------------------------------------------------------------------------------- E5
def _benchmark_domain():
    src = (V9 / "reused/methodology/benchmark_005.py").read_text()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "BENCHMARK_DOMAIN":
            return {k.value: list(eval(compile(ast.Expression(v), "dom", "eval"))) for k, v in zip(node.value.keys, node.value.values)}
    raise RuntimeError("BENCHMARK_DOMAIN not found")


def e5():
    wl = parse_workload()
    stmts = []
    for g in wl["groups"]:
        for s in g["statements"]:
            stmts.append({"statement_id": s["statement_id"], "group": g["name"], "nose_text": s["text"], **parse_statement(s["text"])})
    stmts.sort(key=lambda x: x["statement_id"])
    weights = {g["name"]: g["weights"] for g in wl["groups"]}
    norm = {w: {g: weights[g][w] / sum(weights[x][w] for x in weights) for g in weights} for w in WORKLOADS}
    freq = {w: {s["statement_id"]: norm[w][s["group"]] for s in stmts} for w in WORKLOADS}
    v8w = read_json(V9 / "reused/config/rubis_workloads.json")
    v8_equal = all(abs(weights[g][w] - v8w["weights"][g][i]) < 1e-9 for g in weights for i, w in enumerate(WORKLOADS))
    reg = read_json(V9 / "reused/inputs/rubis_workload.json")
    sha = sha256_file(WORKLOAD_RB)
    dom = _benchmark_domain()
    out = stage_dir("E5") / "WORKLOAD.json"
    rec = {"source": str(WORKLOAD_RB), "source_sha256": sha, "registered_v8_sha256": reg["source_sha256"], "nose_commit": NOSE_COMMIT,
           "levels": {"request_group": "16 grupos com pesos por carga", "statement": "36 instruções numeradas (texto NoSE)",
                      "physical_cql": "definido por esquema na síntese de rotas (E13): QueryPlan/WritePlan"},
           "groups": [{"name": g["name"], "weights": g["weights"], "statements": [s["statement_id"] for s in g["statements"]]} for g in wl["groups"]],
           "statements": stmts, "normalized_group_weights": norm, "statement_frequency_per_request": freq,
           "parameter_domain": {"status": "HIPÓTESE DE PESQUISA (não é dado histórico)", "domain": dom,
                                "sampling": "uniforme com semente (reused/methodology/workload_005.histories, seed 42005 + namespace)",
                                "since": "fixo em 2010-01-01T00:00:00"}}
    write_json(out, rec)
    n_read = sum(s["kind"] == "read" for s in stmts)
    checks = {"sha256_matches_v8": sha == reg["source_sha256"], "groups_16": len(weights) == 16, "statements_36": len(stmts) == 36,
              "reads_28": n_read == 28, "writes_8": len(stmts) - n_read == 8, "weights_equal_v8_config": v8_equal,
              "ids_1_to_36": [s["statement_id"] for s in stmts] == list(range(1, 37))}
    return ("PASS" if all(checks.values()) else "FAIL"), [out, WORKLOAD_RB], checks, {}


# ------------------------------------------------------------------------------------------- E6
def e6():
    from . import oracle_v9 as O
    from .contract import CONTRACT, READ_IDS, WRITE_IDS
    from .schema_map import ATTR_TYPES, FK, REL
    from reused.methodology.oracle_011 import canonical
    from reused.methodology.workload_005 import histories
    wl = read_json(stage_dir("E5") / "WORKLOAD.json")
    nose = {s["statement_id"]: s for s in wl["statements"]}
    # 1) every attribute reference of the contract resolves in the EER
    bad = []

    def check_ref(ent, ref):
        parts = ref.split(".")
        e = ent
        for rel in parts[:-1]:
            if rel not in REL[e]:
                bad.append(f"{ent}:{ref}")
                return
            e = REL[e][rel]
        if parts[-1] not in ATTR_TYPES[e]:
            bad.append(f"{ent}:{ref}")
    for sid, c in CONTRACT.items():
        if c["kind"] == "read":
            for ref, _, _ in c["where"]:
                check_ref(c["entity"], ref)
            for _, ref in c["project"]:
                check_ref(c["entity"], ref)
            for ref, _ in c["order"]:
                check_ref(c["entity"], ref)
        else:
            for a in c["values"]:
                if a not in ATTR_TYPES[c["entity"]]:
                    bad.append(f"{c['entity']}.{a}")
    # 2) oracle vs. the V8 reference run (AUTO_H_norm, matched cohort, 6-design agreement)
    raw_dir = OLD_RUN_011 / "full_scale_matched_cohort/raw"
    cl, s = _session(180)
    try:
        state0, prov = O.load_state(s)
    finally:
        cl.shutdown()
    agree, total, mism = Counter(), Counter(), []
    requests = 0
    for w in WORKLOADS:
        path = raw_dir / f"AUTO_H_norm_{w}.jsonl.gz"
        _v8_input(path, "E6 (referência do oráculo)")
        ref = {}
        with gzip.open(path, "rt") as fh:
            for line in fh:
                r = json.loads(line)
                ref[(r["repetition"], r["logical_request_id"])] = {st["statement_id"]: st["logical_rows"] for st in r["statements"]}
        for rep in range(5):
            set_stage("E6", progress={"text": f"oráculo × referência V8: carga {w}, repetição {rep + 1} de 5", "done": WORKLOADS.index(w) * 5 + rep, "total": 20})
            st = O.fresh(state0)
            for req in histories(w, rep, 1000, _benchmark_domain(), "v9", phase="warmup"):
                ctx = {}
                for sid in req["constituent_statement_ids"]:
                    O.evaluate(st, sid, req["parameters"], ctx)
            for req in histories(w, rep, 10000, _benchmark_domain(), "v9", phase="measured"):
                ctx = {}
                exp_req = ref.get((rep, req["logical_request_id"]))
                requests += 1
                for sid in req["constituent_statement_ids"]:
                    got = canonical(O.evaluate(st, sid, req["parameters"], ctx))
                    total[sid] += 1
                    if exp_req is not None and exp_req.get(sid) == got:
                        agree[sid] += 1
                    elif len(mism) < 40:
                        mism.append({"workload": w, "rep": rep, "request": req["logical_request_id"], "sid": sid,
                                     "expected_v8": (exp_req or {}).get(sid), "oracle_v9": got})
    contract_out = stage_dir("E6") / "SEMANTIC_CONTRACT.json"
    write_json(contract_out, {"statements": {str(k): {**v, "nose_text": nose[k]["nose_text"], "group": nose[k]["group"]} for k, v in CONTRACT.items()},
                              "reads": READ_IDS, "writes": WRITE_IDS,
                              "semantics": "executada na V8 (AUTO_H_norm e cinco desenhos independentes concordantes); desvios do texto NoSE em `deviation`",
                              "canonical_form": "reused/methodology/oracle_011.canonical: float → 2 casas, timestamp → UTC ms"})
    ver_out = stage_dir("E6") / "ORACLE_VERIFICATION.json"
    per = {str(k): {"evaluated": total[k], "agree": agree[k]} for k in sorted(total)}
    write_json(ver_out, {"reference": str(raw_dir), "deployment": "AUTO_H_norm (V8-011, coorte)", "requests": requests,
                         "statement_evaluations": sum(total.values()), "agreements": sum(agree.values()), "per_statement": per,
                         "state_provenance": prov, "first_mismatches": mism, "unresolved_contract_refs": bad})
    checks = {"contract_refs_resolve": not bad, "all_36_statements_evaluated": len(total) == 36,
              "oracle_equals_v8_reference": sum(agree.values()) == sum(total.values()), "requests_200000": requests == 200000,
              "agreement": f"{sum(agree.values())}/{sum(total.values())}"}
    ok = checks["contract_refs_resolve"] and checks["all_36_statements_evaluated"] and checks["oracle_equals_v8_reference"] and checks["requests_200000"]
    return ("PASS" if ok else "FAIL"), [contract_out, ver_out], checks, {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=("E3", "E4", "E5", "E6", "E7"))
    a = ap.parse_args()
    fn = {"E3": e3, "E5": e5, "E6": e6}
    if a.stage in ("E4", "E7"):
        from . import phase1b
        fn.update({"E4": phase1b.e4, "E7": phase1b.e7})
    r = run_stage(a.stage, fn[a.stage])
    print(json.dumps({"stage": a.stage, "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str))
    return 0 if r["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
