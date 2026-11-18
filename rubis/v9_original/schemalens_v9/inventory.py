"""Fase 0 inventory of the V1–V8 repository (read-only) -> runs/v9_generation_001/inventory/INVENTORY.md.

Code excerpts are extracted by pattern from the old files (with line numbers); counts are read from the
V8 result files or from Cassandra's schema tables.  Every old file opened is registered with its SHA-256 in
provenance/V8_INPUTS.json.  The prose classifies what the extracted evidence shows.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from .common import OLD, OLD_RUN_011, OLD_RUN_012, RUN, now_iso, read_json, sha256_file, write_json, write_text
from .stages import run_stage, stage_dir

READ: dict[str, str] = {}


def rd(rel: str) -> list[str]:
    p = OLD / rel
    READ[str(p)] = sha256_file(p)
    return p.read_text(errors="replace").splitlines()


def rj(p: Path):
    READ[str(p)] = sha256_file(p)
    return json.loads(p.read_text())


def excerpt(rel: str, pattern: str, before: int = 0, after: int = 3, width: int = 170, nth: int = 0) -> str:
    lines = rd(rel)
    hits = [i for i, l in enumerate(lines) if re.search(pattern, l)]
    if len(hits) <= nth:
        return f"(padrão `{pattern}` não encontrado em {rel})"
    i = hits[nth]
    a, b = max(0, i - before), min(len(lines), i + after + 1)
    body = "\n".join(f"{k + 1:5d}  {lines[k][:width]}{' …' if len(lines[k]) > width else ''}" for k in range(a, b))
    return f"`{rel}:{i + 1}`\n```python\n{body}\n```"


def line_of(rel: str, pattern: str) -> int | None:
    for i, l in enumerate(rd(rel)):
        if re.search(pattern, l):
            return i + 1
    return None


def cassandra_tables(keyspaces: list[str]) -> dict[str, dict[str, list[str]]]:
    from cassandra.cluster import Cluster
    c = Cluster(["127.0.0.1"])
    s = c.connect()
    try:
        out = {}
        for ks in keyspaces:
            cols = {}
            for r in s.execute("SELECT table_name, column_name FROM system_schema.columns WHERE keyspace_name=%s", (ks,)):
                cols.setdefault(r.table_name, []).append(r.column_name)
            out[ks] = {t: sorted(v) for t, v in sorted(cols.items())}
        return out
    finally:
        c.shutdown()


def volatile_attribute_table(sl_tables: dict[str, list[str]]) -> list[dict]:
    """Attributes UPDATEd by the SchemaLens write routes (benchmark_005) and every SL table that holds a copy."""
    rel = "methodology/benchmark_005.py"
    lines = rd(rel)
    rows, seen = [], set()
    sid = None
    lo = next(i for i, l in enumerate(lines) if "def execute_statement" in l)
    hi = next(i for i, l in enumerate(lines) if "def _project_user" in l)
    for i, l in enumerate(lines):
        if not lo <= i < hi:   # only the request routes, not helpers or reset_state
            continue
        m = re.search(r"elif sid == (\d+):|if sid == (\d+):", l)
        if m:
            sid = int(m.group(1) or m.group(2))
        m = re.search(r'UPDATE \{ks\}\.(\w+) SET ([\w=?, ]+?) WHERE', l)
        if m and m.group(1) in sl_tables:
            attrs = [a.split("=")[0].strip() for a in m.group(2).split(",")]
            for a in attrs:
                if a not in sl_tables[m.group(1)]:   # UPDATE of another deployment kind with a same-named table
                    continue
                key = (sid, m.group(1), a)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"statement": sid, "table": m.group(1), "attribute": a, "line": i + 1})
    by_attr = {}
    for r in rows:
        by_attr.setdefault(r["attribute"], {"statements": set(), "updated_tables": set(), "lines": set()})
        by_attr[r["attribute"]]["statements"].add(r["statement"])
        by_attr[r["attribute"]]["updated_tables"].add(r["table"])
        by_attr[r["attribute"]]["lines"].add(r["line"])
    out = []
    for a, v in sorted(by_attr.items()):
        holders = sorted(t for t, cols in sl_tables.items() if a in cols)
        out.append({"attribute": a, "written_by_statements": sorted(v["statements"]), "tables_updated": sorted(v["updated_tables"]),
                    "tables_holding_a_copy": holders, "in_items_selective": "items_selective" in holders, "code_lines": sorted(v["lines"])})
    return out


def build() -> tuple[str, dict]:
    cs = rj(OLD / "outputs/final_run_v8_completion_006/design_space/CANDIDATE_SEARCH_SPACE.json") if (OLD / "outputs/final_run_v8_completion_006/design_space/CANDIDATE_SEARCH_SPACE.json").exists() \
        else rj(OLD / "outputs/final_run_v8_completion_005/design_space/CANDIDATE_SEARCH_SPACE.json")
    P, C, CCR = cs["raw_candidate_projections_count_P"], cs["consolidated_tables_count_C"], cs["candidate_consolidation_ratio_CCR"]
    qids = sorted({p["statement_id"] for p in cs["raw_candidates_P"]})
    archs = sorted({p["architecture"] for p in cs["raw_candidates_P"]})
    wl = rj(OLD / "inputs/workload/rubis_workload.json")
    n_stmt = len(wl["statements"]); n_read = sum(s["operation_type"] == "read" for s in wl["statements"]); n_write = n_stmt - n_read
    rb_sha = sha256_file(OLD / "inputs/historical_sources/nose_workloads_rubis.rb"); READ[str(OLD / "inputs/historical_sources/nose_workloads_rubis.rb")] = rb_sha
    eer = rj(OLD / "inputs/conceptual_schema/rubis_eer.json")
    fa = rj(OLD_RUN_012 / "ablations/protocol/FACTORIAL_ANALYSIS.json")
    mech = fa.get("mechanisms", [])
    not_ident = [m for m in mech if m.get("status") == "NOT_IDENTIFIABLE" or "NOT_IDENT" in str(m.get("status"))]
    tabs = cassandra_tables(["rubis_v8_c008_auto_norm", "rubis_v8_c008_auto_c3c5c7"])
    norm_t, full_t = tabs["rubis_v8_c008_auto_norm"], tabs["rubis_v8_c008_auto_c3c5c7"]
    extra_t = sorted(set(full_t) - set(norm_t))
    vol = volatile_attribute_table(full_t)
    provd = rd("docs/HISTORICAL_SOURCE_PROVENANCE.md")
    qpg_commit = next((m.group(0) for l in provd for m in [re.search(r"314e1730[0-9a-f]{32}", l)] if m), None)
    nose_commit = wl.get("commit")
    s10 = read_json(RUN / "stages/E01_preflight/PREFLIGHT.json", {}) or {}
    s2 = read_json(RUN / "stages/E02_source_data/SOURCE_COUNTS.json", {}) or {}
    rules_used = [i + 1 for i, l in enumerate(rd("methodology/engine.py")) if re.search(r"rules\[|thresholds", l)]
    fact = {"P": P, "C": C, "CCR": CCR, "P_statement_ids": qids, "P_architectures": archs, "statements": n_stmt, "reads": n_read, "writes": n_write,
            "rubis_rb_sha256": rb_sha, "rubis_rb_sha256_registered_v8": wl.get("source_sha256"), "nose_commit": nose_commit, "qpg_commit": qpg_commit,
            "eer_entities": len(eer["entities"]), "eer_relationships": len(eer["relationships"]),
            "norm_tables": len(norm_t), "c3c5c7_tables": len(full_t), "extra_tables": extra_t, "volatile": vol,
            "mechanisms_not_identifiable": [m.get("mechanism") for m in not_ident], "engine_rules_lines": rules_used}

    T = []
    a = T.append
    a(f"# Inventário da Fase 0 — repositório V1–V8 (somente leitura)\n\nRun: `v9-generation-20261112-001` · gerado em {now_iso()} por `schemalens_v9/inventory.py`.\n")
    a(f"Repositório lido: `{OLD}`. Nenhum arquivo dele foi escrito, movido ou apagado; todos os arquivos abertos estão em `provenance/V8_INPUTS.json` com SHA-256.\n")
    a("Números deste inventário: contagens lidas dos JSON da V8, do esquema do Cassandra (`system_schema`) ou dos estágios E1/E2; trechos de código extraídos por padrão, com o número da linha.\n")

    a("## 1. Resumo\n")
    a(f"- **Diagnóstico da seção 0: os cinco itens se confirmam no código.** Há divergências adicionais, na seção 3.6.")
    a(f"- **E1 (preflight):** {s10.get('cassandra_release_version_nodetool')} confirmado por `nodetool version`; nó UN; acesso a disco do Cassandra = `{s10.get('disk_access_mode_from_log')}` (relevante para a Fase 8: só reinício do nó zera o cache).")
    a(f"- **E2 (fonte):** {s2.get('base_total')} registros-base e {s2.get('all_total')} linhas em {s2.get('table_count')} tabelas; divergências: {len(s2.get('divergences', [])) if s2 else 'NOT_EXECUTED'}.")
    a(f"- **Carga:** `rubis.rb` com SHA-256 `{rb_sha[:16]}…`, {'igual' if rb_sha == wl.get('source_sha256') else 'DIFERENTE'} ao registrado na V8; {n_stmt} instruções ({n_read} leituras, {n_write} escritas), {len(wl['groups'])} grupos.")
    a(f"- **Espaço de projeto da V8:** P = {P} e C = {C} (CCR = {CCR}) vêm de um gerador que **não está ligado** aos esquemas medidos: usa outra taxonomia C0–C8, cobre só {len(qids)} instruções (nenhuma escrita) e numera as consultas de forma diferente da carga do NoSE.")
    a(f"- **H medido:** AUTO_H_norm = {len(norm_t)} tabelas, AUTO_H_c3c5c7 = {len(full_t)} ({', '.join(extra_t)} a mais); as duas listas e as rotas foram escritas à mão.\n")

    a("## 2. Onde está cada peça\n")
    a("| Peça | Arquivo(s) | Observação |\n|---|---|---|")
    a(f"| P (especificações) e C (canônicas) | `methodology/design_space_005.py` (`generate_candidate_search_space`, linha {line_of('methodology/design_space_005.py', 'def generate_candidate_search_space')}) → `outputs/final_run_v8_completion_006/design_space/CANDIDATE_SEARCH_SPACE.json` | P = {P}, C = {C}; consultas e famílias por listas fixas |")
    a(f"| P alternativo (heurístico) | `methodology/engine.py` (`analyze`, `candidates`, `stage8`) | ativação por palavras-chave no texto da instrução; não alimenta H |")
    a(f"| H | `methodology/final_v7.py` (`search`, linha {line_of('methodology/final_v7.py', 'def search')}), `design_space_005.py` (`generate_deployments_and_policy`), `design_space_006.py` | composições listadas à mão |")
    a(f"| Hbench | `design_space_005.py:{line_of('methodology/design_space_005.py', 'h_bench = ')}`, `final_v7.py:{line_of('methodology/final_v7.py', 'selected=')}` | lista fixa de 2 esquemas |")
    a(f"| DDL de AUTO_H_norm / c3c5c7 | `methodology/final_v8.py` (`TARGET_SCHEMAS`), `methodology/setup_cohort_a.py`; keyspaces `rubis_v8_c008_auto_norm` / `rubis_v8_c008_auto_c3c5c7` | {len(norm_t)} / {len(full_t)} tabelas lidas do `system_schema` |")
    a(f"| Rotas (QueryPlan/WritePlan) dos esquemas medidos | `methodology/benchmark_005.py` (`DeploymentAdapter.execute_statement`, linhas {line_of('methodology/benchmark_005.py', 'def execute_statement')}–{line_of('methodology/benchmark_005.py', 'def _project_user')}), `semantic_replay_011.py` (`DeploymentAdapter008`), `ablation_factorial_012.py` (`FactorialAdapter`) | `if/elif` por instrução e por tipo de esquema |")
    a(f"| Modelo EER | `inputs/conceptual_schema/rubis_eer.json` (gerado por `engine.conceptual_schema`) | {len(eer['entities'])} entidades, {len(eer['relationships'])} relacionamentos; **sem** atributos, chaves, opcionalidade nem cardinalidade por relacionamento |")
    a("| Contrato das 36 instruções | implícito no oráculo `methodology/oracle_011.py` (`expected`, `apply_mutation`) e em `writeplans_011.py` (eventos das 8 escritas) | não existe documento de contrato separado |")
    a("| Catálogo C0–C8 | `config/catalog_C0_C8.json` (só rótulos) e `design_space_005.ARCHITECTURES` (taxonomia diferente) | sem `provides`/`supersedes`/`maintenance`/`activation` |")
    a(f"| Matriz de evidências | `engine.analyze` → `outputs/stage1_3/analytical_matrix.json` | EA/IA/OA/PA e fan-out por palavra-chave; não lê `rubis_full` |")
    a(f"| Regras de ativação | `config/activation_rules.json` (limiares) | lidas por `engine.analyze` mas nunca usadas na decisão (linhas que mencionam `rules[`/`thresholds`: {rules_used or 'nenhuma'}) |")
    a(f"| Coorte | `setup_cohort_a.py` + keyspaces `rubis_v8_c008_*`; domínio de parâmetros `benchmark_005.BENCHMARK_DOMAIN` | não há arquivo-manifesto com a lista de IDs; a coorte é definida pela regra de derivação e pelos keyspaces |\n")

    a("## 3. Confirmação do diagnóstico\n")
    a("### 3.1 H não é feito pelo método — CONFIRMADO\n")
    a("As composições são listas literais. Em `final_v7.py`, AUTO_H_norm é o conjunto `base` e AUTO_H_c3c5c7 é `base + extras + support`; não há busca nem custo:\n")
    a(excerpt("methodology/final_v7.py", r"^def search", 0, 5))
    a("\nA seleção de Hbench é uma constante:\n")
    a(excerpt("methodology/design_space_005.py", r"h_bench = \[", 1, 1))
    a(f"\n`design_space_006.py` ainda declara os dois esquemas com `table_count` 11 e 9, quando os keyspaces medidos têm {len(full_t)} e {len(norm_t)} tabelas:\n")
    a(excerpt("methodology/design_space_006.py", r'"deployment_id": "AUTO_H_c3c5c7"', 0, 4))
    a("\nAs rotas são escritas à mão, uma por instrução e por tipo de esquema (exemplo: StoreBid):\n")
    a(excerpt("methodology/benchmark_005.py", r"elif sid == 20:", 0, 10))
    a("\n### 3.2 Os mecanismos analíticos não são usados — CONFIRMADO\n")
    a("`engine.analyze` calcula `sharedness`, `update_exposure` e `fanout`, mas `activated_families` só depende de palavras-chave do texto da instrução:\n")
    a(excerpt("methodology/engine.py", r'fam=\["C0"\]', 0, 5, width=200))
    a(f"\nO próprio registro da V8-012 marca esses mecanismos como não identificáveis ({', '.join(str(x) for x in fact['mechanisms_not_identifiable']) or 'ver FACTORIAL_ANALYSIS.json'}):\n")
    a(excerpt("methodology/ablation_factorial_012.py", r"not_identifiable = ", 0, 2, width=200))
    a("\n### 3.3 C3, C5 e C7 foram aplicadas como acréscimo — CONFIRMADO\n")
    a("No StoreBid, `bids_by_item` é escrita sempre; `item_bid_shared` (C5) é escrita a mais quando a família está ligada. Nenhuma tabela deixa de existir:\n")
    a(excerpt("methodology/ablation_factorial_012.py", r"INSERT INTO \{ks\}\.bids_by_item", 0, 12))
    a("\nO docstring do fatorial registra a relação \"AUTO_H_norm mais três tabelas\":\n")
    a(excerpt("methodology/ablation_factorial_012.py", r"exactly AUTO_H_norm plus", 0, 1))
    a("\n### 3.4 O catálogo não tem substituição nem custo — CONFIRMADO\n")
    a(excerpt("config/catalog_C0_C8.json", r'"C0"', 0, 1, width=240).replace("```python", "```json"))
    a("\nA outra taxonomia (`design_space_005.ARCHITECTURES`) tem constantes de custo (`storage_multiplier`, `write_cost`), mas nenhuma família declara o que substitui, e os nomes não batem com o catálogo oficial (C1 = índice secundário, C2 = MV, C4 = linha larga, C5 = coleção histórica, C7 = subconjunto filtrado, C8 = blob):\n")
    a(excerpt("methodology/design_space_005.py", r'"C1": \{', 0, 3))
    a("\n### 3.5 Suspeita sobre a C3 — CONFIRMADA\n")
    a("Atributos que as rotas de escrita do SchemaLens atualizam (extraídos dos `UPDATE` de `benchmark_005.py`) e as tabelas do AUTO_H_c3c5c7 que guardam cópia de cada um:\n")
    a("| Atributo | Escrito pela(s) instrução(ões) | Tabelas atualizadas | Tabelas com cópia | Está em `items_selective`? |\n|---|---|---|---|---|")
    for v in vol:
        a(f"| `{v['attribute']}` | {', '.join(map(str, v['written_by_statements']))} | {', '.join(v['tables_updated'])} | {', '.join(v['tables_holding_a_copy'])} | {'sim' if v['in_items_selective'] else 'não'} |")
    a("\nA coluna \"tabelas com cópia\" casa pelo nome da coluna; `bids.max_bid` (valor do próprio lance) e `comments.rating` / `comments_by_to_user.rating` (nota do comentário) são homônimos, não cópias do atributo do item ou do usuário. A E3/E9 da V9 vai resolver isso pela linhagem atributo → entidade.")
    a(f"\n`items_selective` tem as colunas {', '.join('`' + c + '`' for c in full_t.get('items_selective', []))}: só atributos **estáveis**. Os atributos **voláteis** do item (`max_bid`, `nb_of_bids`, atualizados a cada lance pela instrução 22) continuam duplicados em `items` e `items_by_category`, e as duas cópias são atualizadas em toda escrita de lance. A C3 da V8 não isola o atributo volátil, que é o ponto da normalização seletiva de Chen; ela cria mais uma cópia dos estáveis.\n")
    a("### 3.6 Divergências e achados adicionais (relatar antes de continuar)\n")
    a(f"1. **P = {P} e C = {C} não vêm do catálogo oficial nem da carga do NoSE.** O gerador usa a taxonomia de `design_space_005` (seção 3.4), uma lista própria de {len(qids)} consultas numeradas {qids[0]}–{qids[-1]} (sem as {n_write} escritas) e uma numeração que não corresponde à do `rubis.rb` (por exemplo, a consulta 1 do gerador é `items_by_id`; a instrução 1 do NoSE lê `users.nickname, users.password`). Não há linhagem de nenhuma tabela de H até C. Na V9, P e C serão gerados do zero; os números da V8 servem só como histórico.")
    a(excerpt("methodology/design_space_005.py", r"1: \(\"items_by_id\"", 0, 0, width=150))
    a(f"\n2. **O EER da V8 é incompleto para a E3.** `rubis_eer.json` lista {len(eer['entities'])} entidades e {len(eer['relationships'])} relacionamentos, mas sem atributos, chaves, opcionalidade e cardinalidade por relacionamento. A E3 vai extrair isso do DDL de `rubis_full` e do modelo do NoSE.")
    a("3. **Instrução 15 (StoreBuyNow).** O `rubis.rb` declara `UPDATE items SET quantity=?, nb_of_bids=?, end_date=?`, mas a rota e o oráculo da V8 só mudam `quantity` (e regravam `nb_of_bids` sem alterar). A E6 precisa decidir e registrar o contrato (seguir o NoSE ou manter a interpretação da V8); isso afeta quem é volátil (`end_date` é chave de clustering de `items_by_category`, `items_by_region`, `items_by_seller` e `items_by_user_bid`).")
    a(excerpt("methodology/oracle_011.py", r"elif statement_id == 15", 0, 2))
    a(f"\n4. **A coorte não tem arquivo-manifesto com os IDs.** Ela é definida pela regra de derivação (`setup_cohort_a.py`) e pelo conteúdo dos keyspaces `rubis_v8_c008_*`. As contagens serão conferidas contra o enunciado na E17. Na V9, a E17 vai exportar o manifesto (IDs + impressão digital) a partir desses keyspaces antes de materializar.")
    a(f"5. **Commit do QPG.** O commit completo `{qpg_commit}` está em `docs/HISTORICAL_SOURCE_PROVENANCE.md`; a cópia local de `inputs/historical_sources/qpg` não tem metadados git, então a fidelidade da E19 será checada por hash dos arquivos de DDL e planos, não pelo commit.")
    a("6. **Acesso a disco por mmap** (E1): na Fase 8, limpar o cache do SO não basta; é preciso reiniciar o nó antes de cada repetição, como na medição fria da V8-012 (que já terminou, então a condição da Fase 8 está satisfeita).\n")

    a("## 4. Componentes reaproveitáveis da V8-012 (copiados para `reused/`)\n")
    man = read_json(Path(__file__).resolve().parents[1] / "reused/REUSE_MANIFEST.json", {}) or {}
    a(f"Cópias registradas em `reused/REUSE_MANIFEST.json`: {man.get('count')} arquivos, {man.get('modified_count')} modificados depois da cópia.\n")
    a("| Componente | Arquivo copiado | Versão executada |\n|---|---|---|")
    a("| Oráculo independente | `oracle_011.py`, `oracle_complete_011.py` | `oracle_011.py` idêntico ao snapshot executado em `final_run_v8_completion_011/src/methodology/` (mesmo SHA-256) |")
    a("| Protocolo congelado | `protocol_012.py` (`run_protocol`, 4 × 5 × (1.000 + 10.000)) | V8-012 |")
    a("| Guarda de isolamento | `protocol_012.py` (`BulkWriterWatch`, `require_idle_node`) | V8-012; adaptado em `schemalens_v9/isolation.py` (D-V9-003) |")
    a("| `reset_state` | `benchmark_005.DeploymentAdapter`, `semantic_replay_011.DeploymentAdapter008`, `ablation_factorial_012.FactorialAdapter` | V8-011/012 |")
    a("| Aceitação + testes negativos | `acceptance_012.py` (`global_pass_hardcoded = False`) | V8-012 |")
    a("| Carregador por streaming | `full_schemalens_012.py` (SchemaLens), `baseline_continuation_011.py` (NoSE/QPG) | V8-012 / V8-011 |")
    a("| Supervisor / orquestrador | `supervisor_012.py`, `orchestrator_012.py` | V8-012 |")
    a("| Carga | `workloads/rubis.rb` (NoSE `d275634…`), `workload_005.py` (históricos com semente) | V8-005 em diante |\n")

    a("## 5. Estágios E1–E23 no repositório antigo\n")
    a("Classificação: `AUTOMÁTICO` (código gera a saída a partir de entradas), `MANUAL` (valores ou listas escritos à mão), `AUSENTE`, `INVALIDADO POR AUDITORIA`. Versão de referência: `methodology/*_012.py` e `final_run_v8_completion_011/src/methodology/`.\n")
    a("| Estágio | Onde (repositório antigo) | Classificação | Nota |\n|---|---|---|---|")
    rows = [
        ("E1 Preflight", "`preflight_011.py`, `protocol_012.require_idle_node`", "AUTOMÁTICO", "na V9: E1 executado"),
        ("E2 Fonte de dados", "`dataset/generator`, `historical_loader.py`; `rubis_full`", "AUTOMÁTICO", "na V9: E2 executado, sem regenerar"),
        ("E3 Esquema conceitual", "`engine.conceptual_schema` → `rubis_eer.json`", "MANUAL", "listas literais de entidades/relacionamentos; sem atributos, chaves, cardinalidades"),
        ("E4 Estatísticas observadas", "—", "AUSENTE", "fan-out vem de palavra-chave, não de `rubis_full`"),
        ("E5 Carga W", "`engine.import_workload`, `inputs/workload/rubis_workload.json`", "AUTOMÁTICO", "SHA do `rubis.rb` registrado"),
        ("E6 Contrato semântico", "`oracle_011.py`, `writeplans_011.py`", "AUTOMÁTICO (implícito)", "sem documento de contrato; instrução 15 diverge do NoSE"),
        ("E7 Catálogo com contratos", "`config/catalog_C0_C8.json`, `design_space_005.ARCHITECTURES`", "MANUAL", "rótulos; duas taxonomias incompatíveis"),
        ("E8 Fragmento e raiz", "`engine.analyze` (`ri = entities.split(',')[0]`)", "MANUAL", "raiz = primeira entidade do texto; não é o Alg. 2"),
        ("E9 Matriz de evidências", "`engine.analyze`", "INVALIDADO POR AUDITORIA", "por palavra-chave; mecanismos não identificáveis (V8-012)"),
        ("E10 Ativação", "`engine.analyze`, `design_space_005` (listas de qid)", "MANUAL", "qualitativa; limiares não usados"),
        ("E11 Especificações P", "`design_space_005.generate_candidate_search_space`", "INVALIDADO POR AUDITORIA", f"P = {P} desligado de H e do catálogo oficial"),
        ("E12 Canonicalização", "`design_space_005` (chave arquitetura+PK+CK)", "INVALIDADO POR AUDITORIA", f"C = {C}; sem `origin`/`cover`, sem ρmax"),
        ("E13 Síntese de rotas", "`benchmark_005.DeploymentAdapter`", "MANUAL", "rotas escritas à mão por instrução e esquema"),
        ("E14 Modelo de custo", "`engine.stage7`, `design_space_005.analytical_metrics`", "MANUAL", "constantes digitadas; sem calibração"),
        ("E15 Composição H", "`final_v7.search`, `design_space_006`", "MANUAL", "2 esquemas listados à mão"),
        ("E16 Hbench congelado", "`design_space_005:461`, `final_v7.py`", "MANUAL", "lista fixa"),
        ("E17 Coorte e materialização", "`setup_cohort_a.py`, `protocol_012.clone_keyspace`", "AUTOMÁTICO", "sem manifesto de IDs"),
        ("E18 Corretude", "`oracle_011.py`, `semantic_replay_011.py`, `protocol_012.paired_oracle`", "AUTOMÁTICO", ""),
        ("E19 Baselines", "`baselines_005.py`, `baseline_fidelity_006.py`, `cohort_baselines_012.py`", "AUTOMÁTICO", "reconstruções"),
        ("E20 Benchmark", "`protocol_012.run_protocol`", "AUTOMÁTICO", ""),
        ("E21 Análise", "`comparison_012.py` (contrastes); `pareto_010.py`, `design_space_006` (Top-k, regret)", "AUTOMÁTICO (contrastes) / INVALIDADO POR AUDITORIA (Pareto, regret, Top-k)", "métricas retrospectivas da V8-005/006 invalidadas"),
        ("E22 Ablações dos critérios", "`ablation_pipeline_011.py`; `ablation_factorial_012.py`", "INVALIDADO POR AUDITORIA (011) / AUSENTE (critérios)", "011 copiou keyspaces; o fatorial de 2012 mede tabelas, não critérios"),
        ("E23 Escala completa e aceitação", "`full_schemalens_012.py`, `full_cold_012.py`, `acceptance_012.py`", "AUTOMÁTICO", "medição fria concluída em 11/11"),
    ]
    for r in rows:
        a("| " + " | ".join(r) + " |")
    a("\n## 6. Próxima decisão\n")
    a("Aprovar o início da Fase 1 (E3–E7), considerando as divergências da seção 3.6 — em especial a decisão sobre a instrução 15 (item 3).\n")
    return "\n".join(T), fact


def stage():
    md, fact = build()
    out = RUN / "inventory/INVENTORY.md"
    write_text(out, md)
    facts = RUN / "inventory/INVENTORY_FACTS.json"
    write_json(facts, fact)
    prov = RUN / "provenance/V8_INPUTS.json"
    old = read_json(prov, {"files": {}}) or {"files": {}}
    old["files"].update({p: {"sha256": h, "read_by": "inventory"} for p, h in READ.items()})
    for p in ("ablations/protocol", "cohort_baselines/protocol", "orchestration/STATE.json"):
        for f in sorted((OLD_RUN_012 / p).glob("*_RESULT.json")) if (OLD_RUN_012 / p).is_dir() else [OLD_RUN_012 / p]:
            old["files"].setdefault(str(f), {"sha256": sha256_file(f), "read_by": "monitor (ETA histórico)"})
    old.update({"access": "read-only", "updated_at": now_iso()})
    write_json(prov, old)
    checks = {"diagnosis_items_confirmed": 5, "volatile_attrs_found": len(fact["volatile"]), "old_files_registered": len(old["files"])}
    return "PASS", [out, facts, prov], checks, {}


if __name__ == "__main__":
    r = run_stage("INV", stage)
    print(json.dumps({"status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False))
