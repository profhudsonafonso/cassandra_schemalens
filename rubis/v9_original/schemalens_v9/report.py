"""runs/v9_generation_001/reports/V9_REPORT.md — only numbers read from stage JSON files."""
from __future__ import annotations

from .common import RUN, now_iso, read_json, write_text
from .stages import STAGES, load_state

W = ("browsing", "bidding", "update10x", "update100x")
WN = {"browsing": "Browsing", "bidding": "Bidding", "update10x": "Update10x", "update100x": "Update100x"}


def J(p):
    return read_json(RUN / "stages" / p, None)


def f(x, d=2):
    return "—" if x is None else f"{x:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def build() -> str:
    st = load_state()
    L = []
    a = L.append
    a(f"# SchemaLens V9 — relatório da execução `v9-generation-20261112-001`\n\nGerado em {now_iso()} por `schemalens_v9/report.py`. Todos os números são lidos dos arquivos de resultado dos estágios; o que não foi executado aparece como `NOT_EXECUTED`.\n")
    a("## 1. Estado dos estágios\n")
    a("| Estágio | Nome | Status | Duração (s) |\n|---|---|---|---|")
    for sid, _, label, _ in STAGES:
        r = st["stages"].get(sid, {})
        a(f"| {sid} | {label} | {r.get('status', 'NOT_EXECUTED') if r.get('status') != 'PENDING' else 'NOT_EXECUTED'} | {f(r.get('duration_s'), 1) if r.get('duration_s') is not None else '—'} |")
    a("\n## 2. O que mudou em relação à V8\n")
    inv = read_json(RUN / "inventory/INVENTORY_FACTS.json", {})
    e6 = J("E06_semantic_contract/ORACLE_VERIFICATION.json") or {}
    c = J("E12_canonicalization/CANONICAL_C.json") or {}
    mech = J("E13_route_synthesis/MECHANISM_USAGE.json") or {}
    a(f"- **Espaço de projeto.** V8: P = {inv.get('P')}, C = {inv.get('C')}, gerados por um módulo desligado de H. V9: P = {c.get('P_size')}, C = {c.get('C_size')} (CCR = {f(c.get('CCR'), 4)}), com linhagem até cada tabela de H.")
    a(f"- **Oráculo.** V8 calculava a resposta exata só das instruções 34 e 35. V9 calcula as 36 a partir de `rubis_full` e reproduz a referência da V8 em {e6.get('agreements')} de {e6.get('statement_evaluations')} avaliações.")
    a("- **Mecanismos.** " + "; ".join(f"{k}: {v.get('level') or ('muda a saída' if v['output_changes'] else 'não muda a saída')}" for k, v in mech.items()) + ".")
    a("- **Rotas.** QueryPlans e WritePlans sintetizados por código (`schemalens_v9/routes.py`) e executados por um executor genérico; nenhuma rota escrita à mão para os esquemas de H.")
    a("- **Famílias como substituição.** O catálogo declara `supersedes`; a composição proíbe uma tabela de família junto com as tabelas que ela substitui.\n")
    cm = J("E14_cost_model/COST_MODEL.json") or {}
    if cm:
        co = cm["coefficients"]
        a("## 3. Modelo de custo (E14)\n")
        a(f"Coeficientes (ms): leitura a = {f(co['a_read_ms'], 4)}, escrita b = {f(co['b_write_ms'], 4)}, linha lida c = {f(co['c_row_ms'], 5)}; constante por requisição d = {f(co['d_request_ms'], 4)}; R² ponderado = {f(co['weighted_r2'], 4)}. Calibrado em {len(cm['deployments'])} esquemas da V8-012.\n")
        a("| Carga | τ (12 esquemas) | τ deixando um de fora | τ só competitivos |\n|---|---|---|---|")
        for w in W:
            a(f"| {WN[w]} | {f(cm['kendall_tau'][w], 3)} | {f(cm['kendall_tau_leave_one_out'][w], 3)} | {f(cm['kendall_tau_competitive_only'][w], 3)} |")
    H = J("E15_composition/H.json")
    if H:
        a("\n## 4. Esquemas gerados (H)\n")
        a("| Esquema | Selecionado por | Tabelas | Famílias | Custo previsto " + " | ".join(WN[w] for w in W) + " |\n|---|---|---|---|" + "---|" * 4)
        for s in H["H"]:
            a(f"| {s['schema_id']} | {'; '.join(sorted(set(s['selected_by'])))} | {s['table_count']} | {', '.join(s['families'])} | " + " | ".join(f(s['predicted_cost'][w]['total'], 3) for w in W) + " |")
        for an in H["anchors"]:
            a(f"\n- Âncora `{an['label']}`: {an['table_count']} tabelas; coincide com {', '.join(an.get('coincides_with') or []) or 'nenhum esquema de H'}.")
        comp = H.get("anchor_vs_v8") or {}
        if comp:
            a(f"- AUTO_H_norm da V8: {comp['v8_count']} tabelas; âncora gerada: {comp['v9_count']} tabelas.")
    fz = J("E16_hbench_frozen/HBENCH_FROZEN.json")
    if fz:
        a(f"\nHbench congelado em {fz['frozen_at']} (hash `{fz['content_sha256'][:16]}…`), |Hbench| = {fz['Hbench_size']}, DeploymentReduction = {f(fz['DeploymentReduction'], 2)}.\n")
    e18, e19 = J("E18_correctness/CORRECTNESS.json"), J("E19_baselines/BASELINES.json")
    if e18:
        a("## 5. Corretude (E18, E19)\n")
        a("| Esquema | Concorda com o oráculo V9 | Concorda com a referência V8 |\n|---|---|---|")
        for k, v in e18.items():
            a(f"| {k} | {v['agree_oracle']}/{v['evaluations']} | {v['agree_v8_reference']}/{v['evaluations']} |")
        if e19:
            for k, v in e19["correctness"].items():
                a(f"| {k} | {v['agree_oracle']}/{v['evaluations']} | {v['agree_v8_reference']}/{v['evaluations']} |")
    an = J("E21_analysis/ANALYSIS.json")
    a("\n## 6. Benchmark na coorte (E20, E21)\n")
    if not an:
        a("`NOT_EXECUTED` (ou em andamento).")
    else:
        a("Latência média por requisição (ms, IC 95% t, n = 5), ordem intercalada, mesma tarde (ver janelas abaixo): " + ", ".join(an["order"]) + ".\n")
        a("| Esquema | " + " | ".join(WN[w] for w in W) + " |\n|---|" + "---|" * 4)
        for d in an["order"]:
            a(f"| {d} | " + " | ".join(f"{f(an['per_deployment'][d][w]['mean'], 3)} ± {f(an['per_deployment'][d][w]['mean'] - an['per_deployment'][d][w]['ci95_low'], 3)}" for w in W) + " |")
        a("\nContrastes pareados (esquema de H − referência, ms):\n")
        a("| Esquema | Contra | " + " | ".join(WN[w] for w in W) + " |\n|---|---|" + "---|" * 4)
        for h, refs in an["paired_contrasts"].items():
            for ref, ws in refs.items():
                a(f"| {h} | {ref} | " + " | ".join(f"{ws[w]['verdict_95']} ({f(ws[w]['diff_ms']['mean'], 3)}; {f(100 * ws[w]['relative'], 1)}%)" for w in W) + " |")
        a("\nPrevisto × medido (dentro de H):\n")
        a("| Carga | Ordem prevista | Ordem medida | τ | Top-1 preservado | Regret (ms) | A até 5% do melhor |\n|---|---|---|---|---|---|---|")
        for w in W:
            i = an["internal_H"][w]
            a(f"| {WN[w]} | {' < '.join(i['predicted_order'])} | {' < '.join(i['measured_order'])} | {f(i['kendall_tau_predicted_vs_measured'], 3)} | {'sim' if i['top1_preserved'] else 'não'} | {f(i['regret_ms'], 3)} | {', '.join(i['near_best_5pct'])} |")
        a(f"\nHtop (não dominados nas 4 cargas, só esquemas de H): {', '.join(an['Htop_non_dominated'])}.")
        a("\n**Piso de ruído (pares de esquemas com trabalho idêntico na carga).** Mesmas leituras, escritas e linhas lidas por requisição; a diferença medida não pode ser efeito de esquema:\n")
        a("| Carga | Par | Leituras/req | Diferença medida (ms) |\n|---|---|---|---|")
        ids = [d for d in an["order"] if d.startswith("H")]
        noise = []
        for w in W:
            for i, x in enumerate(ids):
                for y in ids[i + 1:]:
                    px, py = an["per_deployment"][x][w], an["per_deployment"][y][w]
                    if all(abs(px[k] - py[k]) < 1e-9 for k in ("physical_reads_per_request", "physical_writes_per_request", "rows_fetched_per_request")):
                        noise.append(abs(px["mean"] - py["mean"]))
                        a(f"| {WN[w]} | {x} × {y} | {f(px['physical_reads_per_request'], 2)} | {f(px['mean'] - py['mean'], 3)} |")
        if noise:
            a(f"\nMaior diferença entre esquemas de trabalho idêntico: {f(max(noise), 3)} ms. Contrastes menores que isso não devem ser lidos como efeito de esquema.")
        e20 = {d: read_json(RUN / f"stages/E20_benchmark/{d}_RESULT.json", {}) for d in an["order"]}
        a("\nJanelas de medição (início–fim): " + "; ".join(f"{d} {e20[d]['started_at'][11:16]}–{e20[d]['finished_at'][11:16]}" for d in an["order"]) +
          ". Janela contínua única: keyspaces rematerializados de `rubis_full` antes da janela; qualquer medição invalidada descarta a janela inteira (a medição anterior, em dois blocos, está arquivada em `E20_benchmark_run1_two_blocks`).")
    a(extra_sections(f, WN, W))
    a("\n## 10. Limitações\n")
    a("Ver `docs/LIMITATIONS.md`.")
    return "\n".join(L) + "\n"


def extra_sections(f, WN, W) -> str:
    L = []
    a = L.append
    an = read_json(RUN / "stages/E22_criteria_ablations/ABLATIONS_ANALYTIC.json", None)
    ph = read_json(RUN / "stages/E22_criteria_ablations/ABLATIONS_PHYSICAL.json", None)
    a("\n## 7. Ablações dos critérios (E22)\n")
    if not an:
        a("`NOT_EXECUTED`.")
    else:
        a("Cada critério foi neutralizado (D-V9-013) e o gerador foi rodado de novo (ativação → P → C → H).\n")
        a("| Critério | Neutralização | Ativações alteradas | ΔP | ΔC | Esquemas de H alterados | Resultado nulo |\n|---|---|---|---|---|---|---|")
        for k, r in an["criteria"].items():
            a(f"| {k} | {r['neutralisation']} | {r['activations_changed']} | {r['delta_P']} | {r['delta_C']} | {', '.join(r['H_changed']) or '—'} | {'sim' if r['null_result'] else 'não'} |")
        if ph:
            a(f"\nEfeito físico na coorte: os esquemas distintos que mudaram foram materializados, conferidos pelo oráculo "
              f"e medidos na mesma janela, intercalados com {ph['counterpart']} (ordem {', '.join(ph['order'])}). "
              f"Concordância com o oráculo: {', '.join(f'{d} {v}' for d, v in ph['agreement'].items())}; esquemas bloqueados pelo oráculo: {', '.join(ph['blocked_by_oracle']) or 'nenhum'}.\n")
            a("| Esquema | Origem | Rótulo | " + " | ".join(WN[w] for w in W) + " |\n|---|---|---|" + "---|" * 4)
            for d, c in ph["contrasts"].items():
                a(f"| {d} | {', '.join(c['criteria'])} | {c['label']} | " + " | ".join(
                    f"{c['per_workload'][w]['verdict_95']} ({f(c['per_workload'][w]['diff_ms']['mean'], 3)} ms)" for w in W) + " |")
            a(f"\nDiferença = esquema ablado − {ph['counterpart']} (negativo: o ablado é mais rápido). IC 95% sobre as 5 repetições pareadas.")
    fc = read_json(RUN / "stages/E23_full_scale_acceptance/FULL_COLD_COMPARISON.json", None)
    sel = read_json(RUN / "stages/E23_full_scale_acceptance/SELECTION.json", None)
    a("\n## 8. Escala completa a frio (E23)\n")
    if not fc:
        a("`NOT_EXECUTED`.")
    else:
        if sel:
            a(f"Seleção: {', '.join(sel['selected'])}. Regra: {sel['rule']}. Vitórias: {sel['wins']}.\n")
        a(f"Escopo: {fc['scope']}. Ordem: {', '.join(fc['order'])}. Controle de cache: {fc['cache_control']['cycles']} ciclos, "
          f"todos aprovados = {fc['cache_control']['all_ok']}, cache máximo após a limpeza {fc['cache_control']['max_kib_after']} KiB. "
          f"Concordância com as respostas da V8 em escala completa: {', '.join(f'{d} {v}' for d, v in fc['agreement'].items())}.\n")
        a("Latência média (ms):\n")
        a("| Esquema | " + " | ".join(WN[w] for w in W) + " |\n|---|" + "---|" * 4)
        for d, x in fc["per_deployment"].items():
            a(f"| {d} | " + " | ".join(f"{f(x[w]['mean'], 3)}" for w in W) + " |")
        a("\nContrastes pareados (H − baseline):\n")
        a("| Esquema | Contra | " + " | ".join(WN[w] for w in W) + " |\n|---|---|" + "---|" * 4)
        for h, refs in fc["paired_contrasts"].items():
            for ref, ws in refs.items():
                a(f"| {h} | {ref} | " + " | ".join(f"{ws[w]['verdict_95']} ({f(ws[w]['diff_ms']['mean'], 3)}; {f(100 * ws[w]['relative'], 1)}%)" for w in W) + " |")
    acc = read_json(RUN / "ACCEPTANCE_V9.json", None)
    a("\n## 9. Aceitação (recalculada dos dados brutos)\n")
    if not acc:
        a("`NOT_EXECUTED`.")
    else:
        a(f"Status: **{acc['acceptance_status']}** (global_pass_hardcoded = {acc['global_pass_hardcoded']}; avaliado em {acc['evaluated_at']}).\n")
        a("| Gate | Status |\n|---|---|")
        for k, v in acc["gates"].items():
            a(f"| {k} | {v['status']} |")
        m = acc["gates"].get("mechanisms", {})
        if m.get("levels"):
            a("\nNível de efeito de cada mecanismo (D-V9-014): " + "; ".join(f"{k}: {v}" for k, v in m["levels"].items()) + ".")
        a(f"\nTestes negativos: {sum(t['passed'] for t in acc['negative_tests'])} de {len(acc['negative_tests'])} rejeitados como esperado.\n")
        for t in acc["negative_tests"]:
            a(f"- {t['test']} → gate `{t['gate']}`: {'rejeitado' if t['rejected'] else 'NÃO rejeitado'}")
    return "\n".join(L)


def write() -> str:
    p = RUN / "reports/V9_REPORT.md"
    write_text(p, build())
    return str(p)


if __name__ == "__main__":
    print(write())
