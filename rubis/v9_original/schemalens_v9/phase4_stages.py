"""Stage wrappers E15 (composição H) and E16 (Hbench congelado)."""
from __future__ import annotations

import json

from . import compose as K
from .common import RUN, now_iso, read_json, sha256_obj, write_json
from .stages import run_stage, stage_dir

HBENCH_POLICY = ("Hbench = todos os esquemas distintos de H (saída do otimizador: ótimos por carga, fronteira de Pareto e "
                 "k-melhores), medidos na coorte com o protocolo congelado, intercalados na mesma janela com o SL norm da V8, "
                 "NoSE e QPG. Âncoras que coincidem com um esquema de H não são medidas em separado. Nenhuma regra de "
                 "ativação, parâmetro do catálogo ou coeficiente do modelo de custo pode mudar depois da primeira medição da V9; "
                 "análises posteriores ficam em arquivo separado marcado como retrospectivo.")


def e15():
    r = K.compose()
    out = stage_dir("E15") / "H.json"
    write_json(out, r)
    summ = [{"name": s["schema_id"] + " · " + s["label"], "tables": s["table_count"], "families": s["families"],
             "predicted_cost": {m: v["total"] for m, v in s["predicted_cost"].items()}} for s in r["H"]]
    write_json(stage_dir("E15") / "H_SUMMARY.json", summ)
    checks = {"H_size": len(r["H"]), "at_least_5": len(r["H"]) >= 5, "all_cover_36_with_writeplans": all(s["valid"] for s in r["H"]),
              "distinct": len({tuple(s["canonical_ids"]) for s in r["H"]}) == len(r["H"]),
              "anchors": {a["label"]: a.get("coincides_with") for a in r["anchors"]}}
    status = "PASS" if checks["at_least_5"] and checks["all_cover_36_with_writeplans"] and checks["distinct"] else "FAIL"
    return status, [out], checks, {"H_size": len(r["H"]), "sha256": sha256_obj([s["canonical_ids"] for s in r["H"]])}


def e16():
    H = read_json(stage_dir("E15") / "H.json")
    bench_dir = RUN / "stages/E20_benchmark"
    measured_before = bench_dir.exists() and any(bench_dir.glob("*_RESULT.json"))
    members = [{"schema_id": s["schema_id"], "label": s["label"], "canonical_ids": s["canonical_ids"],
                "ddl_sha256": sha256_obj(s["ddl"]), "predicted_cost": s["predicted_cost"]} for s in H["H"]]
    frozen = {"run_id": "v9-generation-20261112-001", "frozen_at": now_iso(), "policy": HBENCH_POLICY,
              "policy_sha256": sha256_obj(HBENCH_POLICY), "Hbench": members, "H_size": len(H["H"]), "Hbench_size": len(members),
              "DeploymentReduction": round(1 - len(members) / len(H["H"]), 6),
              "external_in_same_window": ["SL norm da V8 (AUTO_H_norm)", "NoSE", "QPG"],
              "anchors_not_measured_separately": {a["label"]: a.get("coincides_with") for a in H["anchors"]},
              "inputs_sha256": {"H.json": sha256_obj(H), "cost_model": sha256_obj(read_json(RUN / "stages/E14_cost_model/COST_MODEL.json")["coefficients"])},
              "no_v9_measurement_before_freeze": not measured_before}
    frozen["content_sha256"] = sha256_obj({k: v for k, v in frozen.items() if k != "frozen_at"})
    out = stage_dir("E16") / "HBENCH_FROZEN.json"
    write_json(out, frozen)
    checks = {"frozen_before_first_benchmark": not measured_before, "Hbench_size": len(members), "DeploymentReduction": frozen["DeploymentReduction"]}
    return ("PASS" if not measured_before else "FAIL"), [out], checks, {}


if __name__ == "__main__":
    for sid, fn in (("E15", e15), ("E16", e16)):
        r = run_stage(sid, fn)
        print(json.dumps({"stage": sid, "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str))
