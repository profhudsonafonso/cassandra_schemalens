"""Runs E20 in ONE continuous window (an invalidated measurement discards the whole attempt) and then E21."""
from __future__ import annotations

import json
import shutil

from . import phase6, phase6b
from .common import log
from .stages import run_stage, set_overall

MAX_ATTEMPTS = 6


def discard(attempt: int):
    d = phase6.OUT / f"discarded_attempt_{attempt}"
    d.mkdir(parents=True, exist_ok=True)
    for p in phase6.OUT.glob("*_RESULT.json"):
        shutil.move(str(p), d / p.name)
    for p in phase6.OUT.glob("*_INVALID_*.json"):
        shutil.move(str(p), d / p.name)


def prepare():
    """Fresh start of the continuous window: archive any partial result and re-materialise every cohort keyspace
    from rubis_full (an interrupted run can leave writes behind; see the QPG incident)."""
    import time as _t
    from . import phase5 as M, phase5_run as PR
    from .stages import set_stage
    if any(phase6.OUT.glob("*_RESULT.json")):
        discard(f"interrupted_{int(_t.time())}")
    raw, ids, state = PR.cohort()
    H, C = PR.h_schemas()
    cl, s = M.session()
    try:
        for i, h in enumerate(H):
            set_stage("E20", progress={"text": f"recriando keyspaces da coorte ({h['schema_id']})", "done": 0, "total": 1})
            M.materialize_generated(s, PR.ks_of(h["schema_id"]), [C[t] for t in h["canonical_ids"]], state)
        set_stage("E20", progress={"text": "recriando keyspaces da coorte (SL norm V8, NoSE, QPG)", "done": 0, "total": 1})
        M.materialize_v8_norm(s, PR.EXTERNAL["V8_AUTO_H_norm"][0], raw)
        M.materialize_baseline(s, PR.EXTERNAL["NoSE"][0], "nose_bidding", raw)
        M.materialize_baseline(s, PR.EXTERNAL["QPG"][0], "qpg_bidding", raw)
    finally:
        cl.shutdown()
    log("E20: keyspaces da coorte recriados a partir de rubis_full antes da janela contínua")


def main(final_status=True):
    prepare()
    for attempt in range(1, MAX_ATTEMPTS + 1):
        r = run_stage("E20", phase6.e20)
        print(json.dumps({"stage": "E20", "attempt": attempt, "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str), flush=True)
        if r["status"] == "PASS":
            break
        if "invalidated" not in (r["checks"] or {}):
            set_overall("FAILED", f"E20 falhou: {r['error'] or r['checks']}")
            raise SystemExit(1)
        discard(attempt)
        log(f"E20: janela descartada (tentativa {attempt}); recomeçando do zero para manter uma janela contínua")
    else:
        set_overall("FAILED", "E20: medições invalidadas repetidamente por carga concorrente")
        raise SystemExit(1)
    r = run_stage("E21", phase6b.e21)
    print(json.dumps({"stage": "E21", "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str), flush=True)
    if r["status"] != "PASS":
        set_overall("FAILED", "E21 falhou")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
