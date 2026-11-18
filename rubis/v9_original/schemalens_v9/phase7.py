"""Fase 7 — E22: ablations of the activation / composition CRITERIA (not of tables).

For each criterion, neutralise it (D-V9-013), regenerate activation -> P -> C -> H, and record the analytical
effect (activations changed, ΔP, ΔC, which H schemas changed, predicted cost before/after).  Physical effect:
the distinct schemas that changed are materialised in the cohort, checked against the oracle and measured
with the frozen protocol, interleaved with their full-method counterpart in the same window.
Decision baselines: no optional family (anchor), all applicable families as ADDITION, all as SUBSTITUTION
without cost.  Nothing here edits a schema by hand.
"""
from __future__ import annotations

from . import compose as K
from . import phase2 as F
from . import routes as R
from .common import RUN, now_iso, read_json, sha256_obj, write_json
from .phase2_stages import generate
from .stages import stage_dir

CRITERIA = {
    "sharedness": "sharedness fixada em 1 (nenhuma consulta compartilha acesso)",
    "write_exposure": "volatile_proj vazio (nenhum atributo é tratado como volátil)",
    "access_profile": "expected_rows = rows_read_bound = 1, relaxed_ratio = ∞, nonroot_proj_bytes = 0 (perfil de acesso neutro)",
    "support_sharing": "support_attrs e missing_maintenance_keys vazios (sem SupportQuery explícita)",
    "relationships": "reverse_hops = 0 (navegação reversa ignorada)",
    "supersedes": "restrições de substituição removidas (famílias viram acréscimo, como na V8)",
    "cost_selection": "composição sem o modelo calibrado: cada leitura/escrita pesa 1, linhas pesam 0",
    "canonicalization": "sem canonicalização nem fusão (cada especificação é uma tabela)",
}


def _phys(schema):
    return tuple(schema["physical_ids"])


def analytic() -> dict:
    full = K.compose()
    ev0, act0, P0, C0, _ = generate()
    by_label_full = {s["label"]: s for s in full["H"]}
    out = {}
    for k, desc in CRITERIA.items():
        knobs = {k: False}
        ev, act, P, C, _ = generate(knobs)
        changed_act = sorted({(m, s) for m in act for s in act[m] if act[m][s]["active"] != act0[m][s]["active"]})
        r = K.compose(knobs)
        hs = []
        for s in r["H"]:
            ref = by_label_full.get(s["label"])
            same_as_any_full = any(_phys(s) == _phys(f) for f in full["H"])
            hs.append({"label": s["label"], "tables": s["table_count"], "families": s["families"], "physical_ids": s["physical_ids"],
                       "changed_vs_full_same_label": ref is None or _phys(s) != _phys(ref), "identical_to_some_full_schema": same_as_any_full,
                       "predicted_cost": {w: s["predicted_cost"][w]["total"] for w in s["predicted_cost"]},
                       "predicted_cost_full_same_label": {w: ref["predicted_cost"][w]["total"] for w in ref["predicted_cost"]} if ref else None,
                       "valid": s["valid"], "canonical_tables": [t for t in s["canonical_ids"]], "ddl": s["ddl"], "routes": s["routes"]})
        out[k] = {"neutralisation": desc, "activations_changed": len(changed_act), "activation_changes_sample": [list(x) for x in changed_act[:20]],
                  "delta_P": len(P) - len(P0), "delta_C": len(C) - len(C0), "P": len(P), "C": len(C),
                  "H_changed": [h["label"] for h in hs if h["changed_vs_full_same_label"]],
                  "H_new_schemas": [h["label"] for h in hs if not h["identical_to_some_full_schema"]], "H": hs,
                  "null_result": not any(h["changed_vs_full_same_label"] for h in hs),
                  "C_tables": {t["canonical_id"]: t for t in C}}
    return {"generated_at": now_iso(), "full_H": [{"label": s["label"], "physical_ids": s["physical_ids"]} for s in full["H"]], "criteria": out}


def decision_baselines() -> dict:
    """B1: no optional family; B2: all applicable families as addition; B3: all as substitution, no cost."""
    full = K.compose()
    ev, act, P, C, _ = generate()
    by = {t["canonical_id"]: t for t in C}
    b1 = next(a for a in full["anchors"] if a["label"] == "anchor_sl_norm")
    base = set(b1["canonical_ids"])
    optional = {t["canonical_id"] for t in C if set(t["families"]) & K.OPTIONAL and not set(t["families"]) & {"C0", "C1"}}
    b2 = sorted(base | optional)
    superseded = set()
    for tid in optional:
        for sup in by[tid]["supersedes"]:
            for u in C:
                if u["canonical_id"] != tid and any(o["family"] == sup["family"] and o["statement"] == sup["statement"] for o in u["origin_all"]):
                    superseded.add(u["canonical_id"])
    b3 = sorted((base | optional) - superseded)
    out = {}
    for name, tabs, desc in (("B1_no_optional", sorted(base), "nenhuma família opcional (âncora; reproduz o SL norm gerado)"),
                             ("B2_all_addition", b2, "todas as famílias aplicáveis como acréscimo (análogo ao SL c3c5c7 da V8)"),
                             ("B3_all_substitution_no_cost", b3, "todas as famílias aplicáveis como substituição, sem modelo de custo")):
        S = [by[t] for t in tabs]
        pr = K.predicted(S)
        out[name] = {"description": desc, "canonical_ids": tabs, "physical_ids": sorted(by[t]["physical_id"] for t in tabs),
                     "tables": [by[t]["name"] for t in tabs], "table_count": len(tabs), "families": sorted({f for t in S for f in t["families"]}),
                     "ddl": [by[t]["ddl"] for t in tabs], "valid": pr["valid"], "invalid": pr["invalid"], "routes": pr["plans"],
                     "predicted_cost": pr["per_mix"], "identical_to_full": [s["label"] for s in full["H"] if tuple(s["physical_ids"]) == tuple(sorted(by[t]["physical_id"] for t in tabs))]}
    return {"C_tables": {t["canonical_id"]: t for t in C}, "baselines": out}


def physical_plan(an: dict, db: dict) -> list[dict]:
    """Distinct schemas to measure: every new schema of an ablation (+ its full counterpart) and B2/B3 if new."""
    full = {s["label"]: tuple(s["physical_ids"]) for s in an["full_H"]}
    seen, plan = {}, []
    for k, rec in an["criteria"].items():
        for h in rec["H"]:
            if h["identical_to_some_full_schema"] or not h["valid"]:
                continue
            key = tuple(h["physical_ids"])
            if key in seen:
                seen[key]["criteria"].append(k)
                continue
            item = {"id": f"A{len(plan) + 1:02d}", "criteria": [k], "label": h["label"], "physical_ids": list(key),
                    "counterpart_label": h["label"] if h["label"].startswith("opt_") and h["label"] in full else "opt_bidding", "source": "ablation",
                    "canonical_ids": h["canonical_tables"], "c_from": k, "routes": h["routes"], "ddl": h["ddl"]}
            seen[key] = item
            plan.append(item)
    for name, b in db["baselines"].items():
        key = tuple(b["physical_ids"])
        if b["identical_to_full"] or key in seen or not b["valid"]:
            continue
        item = {"id": f"A{len(plan) + 1:02d}", "criteria": [name], "label": name, "physical_ids": list(key), "counterpart_label": "opt_bidding",
                "source": "decision_baseline", "canonical_ids": b["canonical_ids"], "c_from": None, "routes": b["routes"], "ddl": b["ddl"]}
        seen[key] = item
        plan.append(item)
    return plan
