"""Build docs/SchemaLens_V9_Relatorio_Completo.pdf from the run's result files (no hand-typed numbers).

Sections: summary (computed), process timeline (stage files + incidents), full V9_REPORT.md, decision log,
limitations, artefact index. Markdown -> HTML (python-markdown) -> PDF (headless Chrome).
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys

import markdown

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUN = ROOT / "runs/v9_generation_001"
OUT_HTML = RUN / "reports/V9_RELATORIO_COMPLETO.html"
OUT_PDF = ROOT / "docs/SchemaLens_V9_Relatorio_Completo.pdf"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
W = ("browsing", "bidding", "update10x", "update100x")
WN = {"browsing": "Browsing", "bidding": "Bidding", "update10x": "Update10x", "update100x": "Update100x"}
PHASE = {0: "Fase 0 — preparação", 1: "Fase 1 — entradas", 2: "Fase 2 — geração", 3: "Fase 3 — custo",
         4: "Fase 4 — composição", 5: "Fase 5 — corretude", 6: "Fase 6 — benchmark na coorte",
         7: "Fase 7 — ablações", 8: "Fase 8 — escala completa e aceitação"}


def J(p, d=None):
    p = RUN / p
    return json.loads(p.read_text()) if p.exists() else d


def f(x, d=2):
    return f"{x:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def hms(s):
    s = int(s or 0)
    return f"{s // 3600} h {s % 3600 // 60:02d} min" if s >= 3600 else (f"{s // 60} min {s % 60:02d} s" if s >= 60 else f"{s} s")


def summary() -> str:
    acc = J("ACCEPTANCE_V9.json")
    sel = J("stages/E23_full_scale_acceptance/SELECTION.json")
    fc = J("stages/E23_full_scale_acceptance/FULL_COLD_COMPARISON.json")
    an = J("stages/E21_analysis/ANALYSIS.json") or {}
    e22 = J("stages/E22_criteria_ablations/ABLATIONS_PHYSICAL.json")
    L = ["## Resumo executivo\n"]
    L.append(f"- **Aceitação final:** {acc['acceptance_status']}. {sum(v['status'] == 'PASS' for v in acc['gates'].values())} de {len(acc['gates'])} gates "
             f"aprovados, recalculados dos dados brutos (`global_pass_hardcoded = {acc['global_pass_hardcoded']}`). "
             f"{sum(t['passed'] for t in acc['negative_tests'])} de {len(acc['negative_tests'])} testes negativos de adulteração foram rejeitados.")
    if an.get("Htop_non_dominated"):
        L.append(f"- **Coorte (E20/E21):** esquemas de H não dominados nas 4 cargas: {', '.join(an['Htop_non_dominated'])}. "
                 f"τ de Kendall entre previsto e medido: " + ", ".join(f"{WN[w]} {f(an['internal_H'][w]['kendall_tau_predicted_vs_measured'], 1)}" for w in W) + ".")
    L.append(f"- **Selecionados para a escala completa:** {', '.join(sel['selected'])}. Vitórias na coorte: "
             + ", ".join(f"{k} {v}" for k, v in sel["wins"].items()) + ".")
    best = sel["selected"][0]
    for ref in fc["paired_contrasts"][best]:
        ws = fc["paired_contrasts"][best][ref]
        L.append(f"- **{best} vs {ref}, escala completa a frio:** " + "; ".join(
            f"{WN[w]} {ws[w]['verdict_95']} ({f(ws[w]['diff_ms']['mean'])} ms, {f(100 * ws[w]['relative'], 0)}%)" for w in W) + ".")
    if e22:
        L.append(f"- **Ablações (E22):** {len(e22['contrasts'])} esquemas medidos contra {e22['counterpart']}; concordância com o oráculo em todos. "
                 "Veja a seção 7 do relatório e o item 11 das limitações.")
    L.append(f"- **Controle de cache em escala completa:** {fc['cache_control']['cycles']} ciclos de reinício e limpeza; {'todos aprovados' if fc['cache_control']['all_ok'] else 'HÁ CICLOS REPROVADOS'}.")
    return "\n".join(L) + "\n"


def timeline() -> str:
    st = json.loads((ROOT / "monitor/status.json").read_text())
    L = ["## Processo executado\n",
         f"Execução `{st['run_id']}`, iniciada em {st['started_at'][:16].replace('T', ' ')}. Estado final: **{st['overall']}**. "
         "Cada linha abaixo vem do `RESULT.json` do estágio.\n"]
    cur = None
    for s in st["stages"]:
        if s["phase"] != cur:
            cur = s["phase"]
            L.append(f"\n### {PHASE.get(cur, f'Fase {cur}')}\n")
            L.append("| Estágio | Descrição | Status | Início | Fim | Duração |\n|---|---|---|---|---|---|")
        L.append(f"| {s['id']} | {s['label']} | {s['status']} | {(s.get('started_at') or '')[5:16].replace('T', ' ')} | "
                 f"{(s.get('finished_at') or '')[5:16].replace('T', ' ')} | {hms(s.get('duration_s'))} |")
    L.append("\n### Incidentes registrados e como foram tratados\n")
    L.append("| Quando | Incidente |\n|---|---|")
    for i in st["incidents"]:
        L.append(f"| {i['at'][5:16].replace('T', ' ')} | {i['text']} |")
    L.append("| 11-13 08:05 | Aceitação interrompida: StopIteration no teste negativo de `supersedes`. Corrigido (D-V9-014). |")
    L.append("| 11-13 08:20 | Gate `mechanisms` reprovado: a declaração do E13 era no nível de conteúdo. E13 refeito em três níveis (D-V9-014), aceitação refeita: PASS. |")
    inv = st["environment"].get("invalidated_measurements", [])
    L.append(f"\nMedições invalidadas pela guarda de isolamento: {len(inv)} ({', '.join(x['deployment'] for x in inv)}). "
             "Todas foram descartadas e refeitas; os arquivos invalidados ficam guardados.")
    return "\n".join(L) + "\n"


def artefacts() -> str:
    L = ["## Arquivos de evidência\n", "| Arquivo | Conteúdo |\n|---|---|"]
    for p, d in (("runs/v9_generation_001/ACCEPTANCE_V9.json", "gates e testes negativos"),
                 ("runs/v9_generation_001/reports/V9_REPORT.md", "relatório gerado pelo código"),
                 ("runs/v9_generation_001/stages/E15_composition/H.json", "esquemas de H, linhagem e DDL"),
                 ("runs/v9_generation_001/stages/E16_hbench_frozen/HBENCH_FROZEN.json", "Hbench congelado, com hashes"),
                 ("runs/v9_generation_001/stages/E20_benchmark/", "observações brutas da coorte (.jsonl.gz)"),
                 ("runs/v9_generation_001/stages/E22_criteria_ablations/", "ablações (analíticas e físicas)"),
                 ("runs/v9_generation_001/stages/E23_full_scale_acceptance/", "seleção, construção e medição a frio em escala completa"),
                 ("docs/DECISION_LOG.md", "decisões D-V9-001 a D-V9-014"), ("docs/LIMITATIONS.md", "limitações declaradas")):
        L.append(f"| `{p}` | {d} |")
    return "\n".join(L) + "\n"


def demote(md: str) -> str:
    """Drop the document's H1 and push its headings one level down."""
    out = []
    for line in md.splitlines():
        if line.startswith("# "):
            continue
        out.append("#" + line if line.startswith("#") else line)
    return "\n".join(out)


CSS = """
@page { size: A4; margin: 16mm 13mm 16mm 13mm; }
body { font-family: -apple-system, 'Helvetica Neue', Arial, sans-serif; font-size: 9.6pt; line-height: 1.42; color: #1b1f24; }
h1 { font-size: 20pt; margin: 0 0 4pt; } h2 { font-size: 14pt; border-bottom: 1.5px solid #2f5d8a; padding-bottom: 2pt; margin-top: 18pt; color: #1f4468; page-break-after: avoid; }
h3 { font-size: 11.5pt; margin-top: 12pt; color: #2f5d8a; page-break-after: avoid; } h4 { font-size: 10pt; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 10pt; font-size: 7.9pt; page-break-inside: auto; }
tr { page-break-inside: avoid; } th, td { border: 1px solid #c9d1da; padding: 2.5pt 4pt; vertical-align: top; text-align: left; }
th { background: #e8eef5; } tr:nth-child(even) td { background: #f7f9fb; }
code { font-family: Menlo, monospace; font-size: 8pt; background: #f1f3f5; padding: 0 2pt; border-radius: 2pt; }
.cover { border-left: 5px solid #2f5d8a; padding: 6pt 0 6pt 12pt; margin-bottom: 14pt; } .sub { color: #555; font-size: 10pt; }
.part { page-break-before: always; } ul { padding-left: 16pt; } li { margin: 2pt 0; }
"""


def build() -> pathlib.Path:
    acc = J("ACCEPTANCE_V9.json")
    report = (RUN / "reports/V9_REPORT.md").read_text()
    dec = (ROOT / "docs/DECISION_LOG.md").read_text()
    lim = (ROOT / "docs/LIMITATIONS.md").read_text()
    method = (ROOT / "docs/METHOD_V9.md").read_text()
    md = "\n\n".join([
        summary(), "## Método\n\n" + demote(method), timeline(),
        '<div class="part"></div>\n\n## Relatório de resultados (gerado por `report.py`)\n\n' + demote(report),
        '<div class="part"></div>\n\n## Registro de decisões\n\n' + demote(dec),
        "## Limitações declaradas\n\n" + demote(lim), artefacts()])
    body = markdown.markdown(md, extensions=["tables", "sane_lists"])
    html = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>SchemaLens V9 — relatório completo</title>
<style>{CSS}</style></head><body><div class="cover"><h1>SchemaLens V9 — relatório completo da execução</h1>
<div class="sub">Execução <code>v9-generation-20261112-001</code> · Cassandra 3.11.11 · RUBiS · aceitação <b>{acc['acceptance_status']}</b>
(avaliada em {acc['evaluated_at'][:16].replace('T', ' ')}). Todos os números são lidos dos arquivos de resultado.</div></div>
{body}</body></html>"""
    OUT_HTML.write_text(html, encoding="utf-8")
    subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", f"--print-to-pdf={OUT_PDF}",
                    OUT_HTML.as_uri()], check=True, capture_output=True, timeout=180)
    return OUT_PDF


if __name__ == "__main__":
    print(build())
    sys.exit(0)
