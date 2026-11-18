"""Real-time monitor: monitor/status.json + monitor/MONITOR.html (data embedded, refresh 15 s).

ETA rules (no invented numbers):
  * running stage with a work counter -> remaining work / rate measured in this run (moving average, last 5 min);
  * stage not started -> duration of the equivalent V8-012 stage, read from the old repository's records and
    scaled by the volume of work; the source of every estimate is written to monitor/ETA_SOURCES.json;
  * otherwise "sem estimativa".
Run once:  python -m schemalens_v9.monitor        Loop:  python -m schemalens_v9.monitor --loop
"""
from __future__ import annotations

import argparse
import html
import json
import statistics
import time
from datetime import datetime, timedelta
from pathlib import Path

from . import isolation
from .common import LOG, MONITOR_DIR, OLD_RUN_012, RUN, RUN_ID, now_iso, read_json, write_json, write_text
from .stages import PHASES, STAGES, STOP_AFTER, load_state

INTERVAL = 15
RATES = MONITOR_DIR / ".rate_samples.json"


def _dur_min(rec: dict) -> float | None:
    a, b = rec.get("started_at"), rec.get("finished_at")
    if not (a and b):
        return None
    f = lambda x: datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S")
    return (f(b) - f(a)).total_seconds() / 60


def historical_sources() -> dict:
    """Per-stage historical estimate (minutes) read from V8-012 records (old repository, read-only)."""
    src: dict[str, dict] = {}
    sl = [d for p in sorted((OLD_RUN_012 / "ablations/protocol").glob("f*_RESULT.json")) if (d := _dur_min(read_json(p, {}))) is not None]
    base = {b: _dur_min(read_json(OLD_RUN_012 / f"cohort_baselines/protocol/{b}_RESULT.json", {})) for b in ("nose_bidding", "qpg_bidding")}
    h_out = read_json(RUN / "stages/E15_composition/RESULT.json", {}) or {}
    n_h = h_out.get("H_size")
    if sl and all(base.values()):
        per = statistics.median(sl)
        n = n_h if n_h else 5
        src["E20"] = {"minutes": round(per * (n + 1) + sum(base.values()), 1), "origin": "histórico V8",
                      "basis": f"mediana de {len(sl)} medições SchemaLens na coorte da V8-012 ({per:.1f} min cada) × ({'|H| = ' + str(n_h) if n_h else 'mínimo de 5 esquemas de H'} + SL norm) + NoSE + QPG medidos na coorte da V8-012",
                      "files": [str(OLD_RUN_012 / "ablations/protocol"), str(OLD_RUN_012 / "cohort_baselines/protocol")]}
    # E22: V9's own measured durations (E18 replay per schema, E20 timed run per schema) × schemas to measure
    e18 = read_json(RUN / "stages/E18_correctness/RESULT.json", {}) or {}
    plan = read_json(RUN / "stages/E22_criteria_ablations/PHYSICAL_PLAN.json", None)
    e20s = [r for r in (read_json(p, {}) for p in sorted((RUN / "stages/E20_benchmark_run1_two_blocks").glob("H*_RESULT.json")))]
    durs = [_dur_min(r) for r in e20s if _dur_min(r)]
    if e18.get("duration_s") and durs:
        n_abl = len(plan) if plan else 5
        per_t = statistics.median(durs)
        m = n_abl * (e18["duration_s"] / 60 / 5) + 10 + (n_abl + 1) * per_t
        src["E22"] = {"minutes": round(m, 1), "origin": "medida na V9",
                      "basis": f"{n_abl} esquemas de ablação: materialização + verificação ({e18['duration_s'] / 300:.1f} min cada, E18) + 10 min de nó ocioso + {n_abl + 1} medições × {per_t:.1f} min (mediana do E20)",
                      "files": [str(RUN / "stages/E18_correctness/RESULT.json"), str(RUN / "stages/E20_benchmark_run1_two_blocks")]}
    e21 = read_json(RUN / "stages/E21_analysis_run1_two_blocks/RESULT.json", {}) or {}
    if e21.get("duration_s") is not None:
        src["E21"] = {"minutes": round(e21["duration_s"] / 60, 2), "origin": "medida na V9", "basis": "duração do E21 anterior", "files": []}
    st = (read_json(OLD_RUN_012 / "orchestration/STATE.json", {}) or {}).get("tasks", {})
    parts = {k: _dur_min(st.get(k, {})) for k in ("full_build", "full_verify", "cold_load_nose", "cold_load_qpg", "cold_bench")}
    if all(parts.values()):
        m = 2 * parts["full_build"] + 2 * parts["full_verify"] + parts["cold_load_nose"] + parts["cold_load_qpg"] + parts["cold_bench"]
        src["E23"] = {"minutes": round(m, 1), "origin": "histórico V8",
                      "basis": "V8-012: 2 construções completas de keyspace SchemaLens (uma por esquema de H) + verificação, carga de NoSE e QPG, medição fria de 4 esquemas",
                      "files": [str(OLD_RUN_012 / "orchestration/STATE.json")]}
    for sid in [s[0] for s in STAGES]:
        src.setdefault(sid, {"minutes": None, "origin": "sem estimativa", "basis": "sem etapa equivalente com duração registrada na V8-012"})
    return src


def _rate_eta(sid: str, prog: dict) -> tuple[float | None, str]:
    samples = read_json(RATES, {}) or {}
    now = time.time()
    lst = [x for x in samples.get(sid, []) if now - x[0] <= 300]
    if prog.get("done") is not None:
        lst.append([now, prog["done"]])
    samples[sid] = lst
    write_json(RATES, samples)
    if len(lst) >= 2 and prog.get("total") and lst[-1][1] > lst[0][1]:
        rate = (lst[-1][1] - lst[0][1]) / (lst[-1][0] - lst[0][0])
        return (prog["total"] - prog["done"]) / rate / 60, "medida"
    return None, "sem estimativa"


def build_status() -> dict:
    st = load_state()
    hist = historical_sources()
    write_json(MONITOR_DIR / "ETA_SOURCES.json", {"generated_at": now_iso(), "sources": hist})
    rows, machine_min, unknown = [], 0.0, []
    first_results_reached = False
    for sid, folder, label, phase in STAGES:
        rec = st["stages"].get(sid, {})
        status = rec.get("status", "PENDING")
        eta, origin = None, "—"
        if status == "RUNNING" and rec.get("progress"):
            eta, origin = _rate_eta(sid, rec["progress"])
            if eta is None and hist[sid]["minutes"] is not None:   # e.g. still waiting for an idle node
                eta, origin = hist[sid]["minutes"], hist[sid]["origin"] + " (etapa ainda sem taxa medida)"
        elif status in ("PENDING", "WAITING_APPROVAL", "BLOCKED"):
            eta, origin = hist[sid]["minutes"], hist[sid]["origin"]
        if not first_results_reached and status not in ("PASS", "SKIPPED"):
            if eta is None:
                unknown.append(sid)
            else:
                machine_min += eta
        if sid == "E21":
            first_results_reached = True
        rows.append({"id": sid, "label": label, "phase": phase, "status": status, "started_at": rec.get("started_at"),
                     "finished_at": rec.get("finished_at"), "duration_s": rec.get("duration_s"),
                     "progress": (rec.get("progress") or {}).get("text"), "eta_min": None if eta is None else round(eta, 1),
                     "eta_origin": origin, "stop_point": STOP_AFTER.get(sid), "error": rec.get("error")})
    waits = [r for r in rows if r["stop_point"] and r["id"] in ("E2", "E16")]
    approvals_pending = sum(1 for r in waits if st["stages"].get(r["id"], {}).get("approved") is not True)
    env = {"node": isolation.node_status(), "isolation": isolation.snapshot(), "disk": isolation.disk(),
           "invalidated_measurements": st.get("invalidated_measurements", [])}
    h = read_json(RUN / "stages/E15_composition/H_SUMMARY.json", None)
    partial = read_json(RUN / "stages/E20_benchmark/PARTIAL.json", None)
    try:
        tail = LOG.read_text().splitlines()[-20:]
    except FileNotFoundError:
        tail = []
    eta_end = (datetime.now() + timedelta(minutes=machine_min)).strftime("%d/%m %H:%M") if not unknown else None
    rem_all = sum(r["eta_min"] for r in rows if r["status"] not in ("PASS", "SKIPPED", "FAIL") and r["eta_min"] is not None)
    unk_all = [r["id"] for r in rows if r["status"] not in ("PASS", "SKIPPED") and r["eta_min"] is None]
    return {"run_id": RUN_ID, "started_at": st.get("started_at"), "updated_at": now_iso(), "updated_epoch": time.time(),
            "overall": st.get("overall"), "overall_note": st.get("overall_note"), "current": st.get("current"),
            "first_results": {"milestone": "fim da Fase 6 (E20–E21: benchmark e análise na coorte)",
                              "machine_minutes_known": round(machine_min, 1), "stages_without_estimate": unknown,
                              "eta_wallclock_if_no_waits": eta_end, "approval_wait": f"{approvals_pending} parada(s) pendente(s); sem estimativa (depende de aprovação)"},
            "all_remaining": {"minutes": round(rem_all, 1), "eta": (datetime.now() + timedelta(minutes=rem_all)).strftime("%d/%m %H:%M"), "without_estimate": unk_all},
            "stages": rows, "environment": env, "H": h, "partial_results": partial, "log_tail": tail, "incidents": st.get("incidents", [])}


CSS = """:root{--bg:#f7f6f2;--card:#fff;--ink:#1d1d1b;--mut:#6b6a63;--line:#e2e0d8;--ok:#2f7d4f;--run:#2563c9;--warn:#a16207;--fail:#b4441a}
@media (prefers-color-scheme: dark){:root{--bg:#18181b;--card:#222226;--ink:#ececec;--mut:#a3a3a3;--line:#3a3a40}}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}main{max-width:1180px;margin:0 auto;padding:16px}
h1{font-size:20px;margin:4px 0}h2{font-size:15px;margin:18px 0 6px}.sub{color:var(--mut);font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-top:10px}.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px}
.k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.04em}.v{font-size:16px;font-weight:600}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:8px;overflow:hidden;font-size:13px}
th,td{padding:6px 8px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}th{color:var(--mut);font-weight:600;font-size:12px}
.pill{display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;font-weight:600;color:#fff}
.PASS{background:var(--ok)}.RUNNING{background:var(--run)}.FAIL,.BLOCKED{background:var(--fail)}.WAITING_APPROVAL{background:var(--warn)}.PENDING,.SKIPPED{background:#8a8a85}
.stop td{border-top:2px solid var(--warn)}.alert{border-left:4px solid var(--fail)}pre{white-space:pre-wrap;font-size:12px;margin:0}.tbl{overflow-x:auto}"""


def _e(x) -> str:
    return html.escape("" if x is None else str(x))


def render(s: dict) -> str:
    ph = {}
    for r in s["stages"]:
        ph.setdefault(r["phase"], []).append(r)
    trs = []
    for p in sorted(ph):
        trs.append(f"<tr><th colspan=8>{_e(PHASES[p])}</th></tr>")
        for r in ph[p]:
            eta = "—" if r["status"] in ("PASS", "SKIPPED", "FAIL") else (f"{r['eta_min']} min" if r["eta_min"] is not None else "sem estimativa")
            trs.append(f"<tr><td>{_e(r['id'])}</td><td>{_e(r['label'])}</td><td><span class='pill {r['status']}'>{_e(r['status'])}</span></td>"
                       f"<td>{_e((r['started_at'] or '')[11:19])}</td><td>{_e((r['finished_at'] or '')[11:19])}</td>"
                       f"<td>{_e(r['duration_s'] if r['duration_s'] is not None else '')}{' s' if r['duration_s'] is not None else ''}</td>"
                       f"<td>{_e(r['progress'] or (r['error'] or ''))}</td><td>{_e(eta)}<br><span class='sub'>{_e(r['eta_origin'])}</span></td></tr>")
            if r["stop_point"]:
                trs.append(f"<tr class='stop'><td colspan=8><b>⏸ {_e(r['stop_point'])}</b></td></tr>")
    env = s["environment"]
    fr = s["first_results"]
    h = s.get("H")
    h_html = "<p class='sub'>H ainda não gerado (Fase 4).</p>"
    if h:
        h_html = "<div class='tbl'><table><tr><th>Esquema</th><th>Tabelas</th><th>Famílias</th><th>Custo previsto</th></tr>" + "".join(
            f"<tr><td>{_e(x['name'])}</td><td>{_e(x['tables'])}</td><td>{_e(', '.join(x['families']))}</td><td>{_e(x.get('predicted_cost'))}</td></tr>" for x in h) + "</table></div>"
    pr = s.get("partial_results")
    pr_html = "<p class='sub'>Sem resultados parciais ainda (Fase 6).</p>" if not pr else f"<pre>{_e(json.dumps(pr, indent=1, ensure_ascii=False))}</pre>"
    inc = "".join(f"<li>{_e(i['at'][:19])} — {_e(i['text'])}</li>" for i in s["incidents"][-15:]) or "<li>nenhum</li>"
    data = json.dumps(s, ensure_ascii=False, default=str).replace("</", "<\\/")
    return f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="{INTERVAL}"><title>Monitor V9</title><style>{CSS}</style></head><body><main>
<h1>Monitor — SchemaLens V9 ({_e(s['run_id'])})</h1>
<div class="sub">Início {_e((s['started_at'] or '')[:19])} · atualizado <span id="upd">{_e(s['updated_at'][:19])}</span> · recarrega a cada {INTERVAL} s</div>
<div id="stale" class="card alert" style="display:none;margin-top:8px"><b>Atenção:</b> mais de 2 min sem atualização — o monitor pode ter parado.</div>
<div class="grid">
<div class="card"><div class="k">Estado geral</div><div class="v"><span class="pill {_e(s['overall'])}">{_e(s['overall'])}</span></div><div class="sub">{_e(s.get('overall_note') or '')}</div></div>
<div class="card"><div class="k">Etapa atual</div><div class="v">{_e(s['current'] or '—')}</div></div>
<div class="card"><div class="k">Primeiros resultados da coorte</div><div class="v">{_e(fr['eta_wallclock_if_no_waits'] or 'parcial')}</div>
<div class="sub">tempo de máquina conhecido: {_e(fr['machine_minutes_known'])} min · etapas sem estimativa: {_e(', '.join(fr['stages_without_estimate']) or 'nenhuma')}<br>aguardando aprovação: {_e(fr['approval_wait'])}<br>marco: {_e(fr['milestone'])}</div></div>
<div class="card"><div class="k">Fim previsto de todas as etapas (passos 1–3)</div><div class="v">{_e(s['all_remaining']['eta'])}</div><div class="sub">tempo de máquina restante: {_e(round(s['all_remaining']['minutes'] / 60, 1))} h · sem estimativa: {_e(', '.join(s['all_remaining']['without_estimate']) or 'nenhuma')}</div></div>
<div class="card"><div class="k">Cassandra</div><div class="v">{'UP/NORMAL' if env['node']['up_normal'] else 'ATENÇÃO'}</div><div class="sub">compactações pendentes: {_e(env['isolation']['pending_compactions'])} · disco livre: {_e(env['disk']['host_free_gb'])} GB</div></div>
<div class="card"><div class="k">Guarda de isolamento</div><div class="v">{'ocioso' if env['isolation']['idle'] else 'OCUPADO'}</div><div class="sub">último check {_e(env['isolation']['checked_at'][11:19])} · escritores: {len(env['isolation']['bulk_writers'])} · medições invalidadas: {len(env['invalidated_measurements'])}</div></div>
</div>
<h2>Etapas (E1–E23)</h2><div class="tbl"><table><tr><th>ID</th><th>Etapa</th><th>Status</th><th>Início</th><th>Fim</th><th>Duração</th><th>Progresso</th><th>ETA (origem)</th></tr>{''.join(trs)}</table></div>
<h2>Esquemas gerados (H)</h2>{h_html}
<h2>Resultados parciais</h2>{pr_html}
<h2>Incidentes</h2><ul>{inc}</ul>
<h2>Últimas 20 linhas do log</h2><div class="card"><pre>{_e(chr(10).join(s['log_tail']))}</pre></div>
<script id="data" type="application/json">{data}</script>
<script>var s=JSON.parse(document.getElementById('data').textContent);if(Date.now()/1000-s.updated_epoch>120)document.getElementById('stale').style.display='block';</script>
</main></body></html>"""


def once() -> dict:
    s = build_status()
    write_json(MONITOR_DIR / "status.json", s)
    write_text(MONITOR_DIR / "MONITOR.html", render(s))
    return s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    a = ap.parse_args()
    while True:
        try:
            once()
        except Exception as exc:  # keep the monitor alive; error is visible in status
            write_json(MONITOR_DIR / "monitor_error.json", {"at": now_iso(), "error": repr(exc)})
        if not a.loop:
            return 0
        time.sleep(INTERVAL)


if __name__ == "__main__":
    raise SystemExit(main())
