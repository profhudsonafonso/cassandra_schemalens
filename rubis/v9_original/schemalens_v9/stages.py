"""Stage registry (E1–E23) and run state. Every stage writes stages/Exx_<name>/RESULT.json."""
from __future__ import annotations

from typing import Any

from .common import RUN_ID, STAGES_DIR, STATE, Timer, file_ref, log, now_iso, read_json, write_json

PHASES = {0: "Fase 0 — Inventário", 1: "Fase 1 — Entradas formais e catálogo", 2: "Fase 2 — Geração de candidatos",
          3: "Fase 3 — Modelo de custo", 4: "Fase 4 — Composição de H", 5: "Fase 5 — Corretude",
          6: "Fase 6 — Benchmark na coorte", 7: "Fase 7 — Ablações dos critérios", 8: "Fase 8 — Escala completa e aceitação"}

# id, folder name, label, phase
STAGES = [
    ("INV", "INV_inventory", "Inventário do repositório antigo", 0),
    ("E1", "E01_preflight", "Preflight do ambiente", 0),
    ("E2", "E02_source_data", "Fonte de dados rubis_full", 0),
    ("E3", "E03_conceptual_schema", "Esquema conceitual S (EER)", 1),
    ("E4", "E04_observed_stats", "Estatísticas observadas", 1),
    ("E5", "E05_workload", "Carga W (rubis.rb)", 1),
    ("E6", "E06_semantic_contract", "Contrato semântico das 36 instruções", 1),
    ("E7", "E07_catalog", "Catálogo com contratos", 1),
    ("E8", "E08_fragments_roots", "Fragmentos e raízes", 2),
    ("E9", "E09_evidence_matrix", "Matriz de evidências", 2),
    ("E10", "E10_activation", "Ativação A(q)", 2),
    ("E11", "E11_specifications", "Especificações P", 2),
    ("E12", "E12_canonicalization", "Canonicalização e fusão", 2),
    ("E13", "E13_route_synthesis", "Síntese de rotas", 2),
    ("E14", "E14_cost_model", "Modelo de custo calibrado", 3),
    ("E15", "E15_composition", "Composição H", 4),
    ("E16", "E16_hbench_frozen", "Hbench congelado", 4),
    ("E17", "E17_cohort_materialization", "Coorte e materialização", 5),
    ("E18", "E18_correctness", "Corretude (oráculo + replay)", 5),
    ("E19", "E19_baselines", "Baselines NoSE e QPG", 5),
    ("E20", "E20_benchmark", "Benchmark na coorte", 6),
    ("E21", "E21_analysis", "Análise", 6),
    ("E22", "E22_criteria_ablations", "Ablações dos critérios", 7),
    ("E23", "E23_full_scale_acceptance", "Escala completa, aceitação e relatório", 8),
]
STOP_AFTER = {"E2": "Parada 1 — fim da Fase 0 (aguarda aprovação)", "E16": "Parada 2 — fim da Fase 4 (aguarda aprovação)"}
BY_ID = {s[0]: s for s in STAGES}


def stage_dir(sid: str):
    return STAGES_DIR / BY_ID[sid][1]


def load_state() -> dict[str, Any]:
    st = read_json(STATE, None)
    if not st:
        st = {"run_id": RUN_ID, "started_at": now_iso(), "overall": "RUNNING", "current": None,
              "stages": {s[0]: {"status": "PENDING"} for s in STAGES}, "incidents": []}
        write_json(STATE, st)
    return st


def save_state(st: dict[str, Any]) -> None:
    write_json(STATE, st)


def set_stage(sid: str, **kw) -> dict[str, Any]:
    st = load_state()
    st["stages"].setdefault(sid, {}).update(kw)
    if kw.get("status") == "RUNNING":
        st["current"] = sid
    save_state(st)
    return st


def set_overall(status: str, note: str | None = None) -> None:
    st = load_state()
    st["overall"] = status
    if note:
        st["overall_note"] = note
    save_state(st)


def incident(text: str) -> None:
    st = load_state()
    st["incidents"].append({"at": now_iso(), "text": text})
    save_state(st)
    log("INCIDENT " + text)


def run_stage(sid: str, fn, inputs: list = ()) -> dict[str, Any]:
    """Run fn() -> (status, outputs:list[Path], checks:dict, extra:dict); write RESULT.json; update state."""
    set_stage(sid, status="RUNNING", started_at=now_iso(), finished_at=None, progress=None)
    log(f"{sid} RUNNING")
    with Timer() as t:
        try:
            status, outputs, checks, extra = fn()
            err = None
        except Exception as exc:  # recorded, not hidden
            status, outputs, checks, extra, err = "FAIL", [], {}, {}, repr(exc)
    res = {"run_id": RUN_ID, "stage": sid, "name": BY_ID[sid][2], "phase": BY_ID[sid][3], "status": status,
           "finished_at": now_iso(), "duration_s": t.seconds,
           "inputs": [file_ref(p) for p in inputs], "outputs": [file_ref(p) for p in outputs],
           "checks": checks, "error": err, **extra}
    write_json(stage_dir(sid) / "RESULT.json", res)
    set_stage(sid, status=status, finished_at=res["finished_at"], duration_s=t.seconds, error=err)
    log(f"{sid} {status} ({t.seconds} s){' ' + err if err else ''}")
    if status == "FAIL":
        incident(f"{sid} falhou: {err or 'verificação reprovada'}")
    return res
