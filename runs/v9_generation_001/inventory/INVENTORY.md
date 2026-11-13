# Inventário da Fase 0 — repositório V1–V8 (somente leitura)

Run: `v9-generation-20261112-001` · gerado em 2026-11-12T11:20:47-0300 por `schemalens_v9/inventory.py`.

Repositório lido: `/Users/hudsonbatista/Documents/rubis3_claude/experiments/rubis_cassandra`. Nenhum arquivo dele foi escrito, movido ou apagado; todos os arquivos abertos estão em `provenance/V8_INPUTS.json` com SHA-256.

Números deste inventário: contagens lidas dos JSON da V8, do esquema do Cassandra (`system_schema`) ou dos estágios E1/E2; trechos de código extraídos por padrão, com o número da linha.

## 1. Resumo

- **Diagnóstico da seção 0: os cinco itens se confirmam no código.** Há divergências adicionais, na seção 3.6.
- **E1 (preflight):** 3.11.11 confirmado por `nodetool version`; nó UN; acesso a disco do Cassandra = `['auto', 'mmap']` (relevante para a Fase 8: só reinício do nó zera o cache).
- **E2 (fonte):** 34200550 registros-base e 70400550 linhas em 13 tabelas; divergências: 0.
- **Carga:** `rubis.rb` com SHA-256 `a4669b5d5b3b1dec…`, igual ao registrado na V8; 36 instruções (28 leituras, 8 escritas), 16 grupos.
- **Espaço de projeto da V8:** P = 86 e C = 41 (CCR = 0.5233) vêm de um gerador que **não está ligado** aos esquemas medidos: usa outra taxonomia C0–C8, cobre só 28 instruções (nenhuma escrita) e numera as consultas de forma diferente da carga do NoSE.
- **H medido:** AUTO_H_norm = 19 tabelas, AUTO_H_c3c5c7 = 22 (item_bid_shared, items_region_bounded, items_selective a mais); as duas listas e as rotas foram escritas à mão.

## 2. Onde está cada peça

| Peça | Arquivo(s) | Observação |
|---|---|---|
| P (especificações) e C (canônicas) | `methodology/design_space_005.py` (`generate_candidate_search_space`, linha 112) → `outputs/final_run_v8_completion_006/design_space/CANDIDATE_SEARCH_SPACE.json` | P = 86, C = 41; consultas e famílias por listas fixas |
| P alternativo (heurístico) | `methodology/engine.py` (`analyze`, `candidates`, `stage8`) | ativação por palavras-chave no texto da instrução; não alimenta H |
| H | `methodology/final_v7.py` (`search`, linha 45), `design_space_005.py` (`generate_deployments_and_policy`), `design_space_006.py` | composições listadas à mão |
| Hbench | `design_space_005.py:461`, `final_v7.py:60` | lista fixa de 2 esquemas |
| DDL de AUTO_H_norm / c3c5c7 | `methodology/final_v8.py` (`TARGET_SCHEMAS`), `methodology/setup_cohort_a.py`; keyspaces `rubis_v8_c008_auto_norm` / `rubis_v8_c008_auto_c3c5c7` | 19 / 22 tabelas lidas do `system_schema` |
| Rotas (QueryPlan/WritePlan) dos esquemas medidos | `methodology/benchmark_005.py` (`DeploymentAdapter.execute_statement`, linhas 536–1223), `semantic_replay_011.py` (`DeploymentAdapter008`), `ablation_factorial_012.py` (`FactorialAdapter`) | `if/elif` por instrução e por tipo de esquema |
| Modelo EER | `inputs/conceptual_schema/rubis_eer.json` (gerado por `engine.conceptual_schema`) | 7 entidades, 10 relacionamentos; **sem** atributos, chaves, opcionalidade nem cardinalidade por relacionamento |
| Contrato das 36 instruções | implícito no oráculo `methodology/oracle_011.py` (`expected`, `apply_mutation`) e em `writeplans_011.py` (eventos das 8 escritas) | não existe documento de contrato separado |
| Catálogo C0–C8 | `config/catalog_C0_C8.json` (só rótulos) e `design_space_005.ARCHITECTURES` (taxonomia diferente) | sem `provides`/`supersedes`/`maintenance`/`activation` |
| Matriz de evidências | `engine.analyze` → `outputs/stage1_3/analytical_matrix.json` | EA/IA/OA/PA e fan-out por palavra-chave; não lê `rubis_full` |
| Regras de ativação | `config/activation_rules.json` (limiares) | lidas por `engine.analyze` mas nunca usadas na decisão (linhas que mencionam `rules[`/`thresholds`: nenhuma) |
| Coorte | `setup_cohort_a.py` + keyspaces `rubis_v8_c008_*`; domínio de parâmetros `benchmark_005.BENCHMARK_DOMAIN` | não há arquivo-manifesto com a lista de IDs; a coorte é definida pela regra de derivação e pelos keyspaces |

## 3. Confirmação do diagnóstico

### 3.1 H não é feito pelo método — CONFIRMADO

As composições são listas literais. Em `final_v7.py`, AUTO_H_norm é o conjunto `base` e AUTO_H_c3c5c7 é `base + extras + support`; não há busca nem custo:

`methodology/final_v7.py:45`
```python
   45  def search(cs):
   46   s=st(); by={c["candidate_id"]:c for c in cs}; base=[c["candidate_id"] for c in cs if c["candidate_id"].startswith("AUTO_") and c["family"] not in ("C3","C5","C7") and c[ …
   47   support=[c["candidate_id"] for c in cs if c["family"]=="C6"]; extras=[c["candidate_id"] for c in cs if c["family"] in ("C3","C5","C7")]
   48   # Deterministic compatible compositions from the generated universe.
   49   combos=[("AUTO_H_norm",base),("AUTO_H_c3c5c7",base+extras+support),("AUTO_H_read",base+support+["AUTO_C8_items_category_compact","AUTO_C8_bids_compact","AUTO_C8_comments …
   50   # compact candidates may be present in V6 catalog; keep only extant IDs.
```

A seleção de Hbench é uma constante:

`methodology/design_space_005.py:461`
```python
  460      # Benchmarked subset chosen by SchemaLens selection policy: H_bench = {AUTO_H_norm, AUTO_H_c3c5c7}
  461      h_bench = ["AUTO_H_norm", "AUTO_H_c3c5c7"]
  462      h_bench_count = len(h_bench)
```

`design_space_006.py` ainda declara os dois esquemas com `table_count` 11 e 9, quando os keyspaces medidos têm 22 e 19 tabelas:

`methodology/design_space_006.py:53`
```python
   53              "deployment_id": "AUTO_H_c3c5c7",
   54              "name": "SchemaLens Hybrid Architecture",
   55              "keyspace": "rubis_v8_full_c3c5c7",
   56              "table_count": 11,
   57              "architectures": ["C0", "C3", "C5", "C6", "C7"],
```

As rotas são escritas à mão, uma por instrução e por tipo de esquema (exemplo: StoreBid):

`methodology/benchmark_005.py:1074`
```python
 1074          elif sid == 20:
 1075              # StoreBid insert
 1076              qty = int(params["qty"])
 1077              bid_val = float(params["bid"])
 1078              if self.kind in ("schemalens_norm", "schemalens_c3c5c7"):
 1079                  self._w(f"INSERT INTO {ks}.bids (id,qty,bid,max_bid,bid_date,user_id,item_id) VALUES (?,?,?,?,?,?,?)",
 1080                          (new_id, qty, bid_val, bid_val, now_dt, uid, iid))
 1081                  self._w(f"INSERT INTO {ks}.bids_by_item (item_id,bid_date,bid_id,user_id,qty,bid) VALUES (?,?,?,?,?,?)",
 1082                          (iid, now_dt, new_id, uid, qty, bid_val))
 1083                  if self.kind == "schemalens_c3c5c7":
 1084                      sel = self._q(f"SELECT name FROM {ks}.items_selective WHERE id=?", (iid,), support=True, candidate_id="AUTO_C3_items_selective")
```

### 3.2 Os mecanismos analíticos não são usados — CONFIRMADO

`engine.analyze` calcula `sharedness`, `update_exposure` e `fanout`, mas `activated_families` só depende de palavras-chave do texto da instrução:

`methodology/engine.py:53`
```python
   53          fam=["C0"]
   54          if op=="read" and ("range" in semantic or "order" in semantic): fam += ["C1","C7"]
   55          if fanout>1: fam += ["C2","C4"]
   56          if op=="write": fam += ["C6"]
   57          if "," in entities: fam += ["C5","C8"]
   58          rows.append({"query_id":sid,"request_group":group,"operation_type":op,"Fi":entities,"ri":entities.split(',')[0],"EA":1 if "equality" in semantic else 0,"IA":1 if op=="write" else 0,"OA":1 if " …
```

O próprio registro da V8-012 marca esses mecanismos como não identificáveis (sharedness, update_exposure, access_profile, support_query_sharing, relationships):

`methodology/ablation_factorial_012.py:321`
```python
  321      not_identifiable = ("H is a hand-listed set of two deployments (design_space_005/006 and semantic_replay_011.DEPLOYMENTS); "
  322                          "engine.analyze computes this signal but activated_families never reads it, so removing it cannot change P, C or H "
  323                          "and no distinct physical deployment exists to measure.")
```

### 3.3 C3, C5 e C7 foram aplicadas como acréscimo — CONFIRMADO

No StoreBid, `bids_by_item` é escrita sempre; `item_bid_shared` (C5) é escrita a mais quando a família está ligada. Nenhuma tabela deixa de existir:

`methodology/ablation_factorial_012.py:121`
```python
  121          self._w(f"INSERT INTO {ks}.bids_by_item (item_id,bid_date,bid_id,user_id,qty,bid) VALUES (?,?,?,?,?,?)",
  122                  (iid, now_dt, new_id, uid, qty, bid_val))
  123          it = self._q(f"SELECT name,end_date FROM {ks}.items WHERE id=?", (iid,), support=True, candidate_id="AUTO_C0_items")
  124          iname = it[0]["name"] if it else f"item-{iid}"
  125          iend = it[0]["end_date"] if it else now_dt
  126          self._w(f"INSERT INTO {ks}.items_by_user_bid (user_id,end_date,item_id,name) VALUES (?,?,?,?)",
  127                  (uid, iend, iid, iname), candidate_id="AUTO_C4_items_by_user_bid")
  128          if "C5" in self.families:
  129              iname_c5 = iname
  130              if "C3" in self.families:
  131                  sel = self._q(f"SELECT name FROM {ks}.items_selective WHERE id=?", (iid,), support=True, candidate_id="AUTO_C3_items_selective")
  132                  iname_c5 = sel[0]["name"] if sel else iname
  133              self._w(f"INSERT INTO {ks}.item_bid_shared (item_id,bid_date,bid_id,item_name,user_id,qty,bid) VALUES (?,?,?,?,?,?,?)",
```

O docstring do fatorial registra a relação "AUTO_H_norm mais três tabelas":

`methodology/ablation_factorial_012.py:9`
```python
    9  The measured SchemaLens deployment AUTO_H_c3c5c7 is exactly AUTO_H_norm plus
   10  three family tables, each with its own read routes and write maintenance:
```

### 3.4 O catálogo não tem substituição nem custo — CONFIRMADO

`config/catalog_C0_C8.json:2`
```json
    2    "C0": "Entity-normalized baseline", "C1": "Direct query-materialized table", "C2": "Decomposed multi-table access", "C3": "Selectively normalized shared entity", "C4": "Reverse-access table", "C5": "Shared multi-query table", "C6": "Maint …
    3  }
```

A outra taxonomia (`design_space_005.ARCHITECTURES`) tem constantes de custo (`storage_multiplier`, `write_cost`), mas nenhuma família declara o que substitui, e os nomes não batem com o catálogo oficial (C1 = índice secundário, C2 = MV, C4 = linha larga, C5 = coleção histórica, C7 = subconjunto filtrado, C8 = blob):

`methodology/design_space_005.py:50`
```python
   50      "C1": {
   51          "name": "Secondary Index / SASI",
   52          "description": "Native secondary index or SASI index on non-primary key column.",
   53          "storage_multiplier": 1.2,
```

### 3.5 Suspeita sobre a C3 — CONFIRMADA

Atributos que as rotas de escrita do SchemaLens atualizam (extraídos dos `UPDATE` de `benchmark_005.py`) e as tabelas do AUTO_H_c3c5c7 que guardam cópia de cada um:

| Atributo | Escrito pela(s) instrução(ões) | Tabelas atualizadas | Tabelas com cópia | Está em `items_selective`? |
|---|---|---|---|---|
| `max_bid` | 22 | items, items_by_category | bids, items, items_by_category | não |
| `nb_of_bids` | 15, 22 | items, items_by_category | items, items_by_category | não |
| `quantity` | 15 | items | items | não |
| `rating` | 27 | users | comments, comments_by_to_user, users | não |

A coluna "tabelas com cópia" casa pelo nome da coluna; `bids.max_bid` (valor do próprio lance) e `comments.rating` / `comments_by_to_user.rating` (nota do comentário) são homônimos, não cópias do atributo do item ou do usuário. A E3/E9 da V9 vai resolver isso pela linhagem atributo → entidade.

`items_selective` tem as colunas `category_id`, `id`, `name`: só atributos **estáveis**. Os atributos **voláteis** do item (`max_bid`, `nb_of_bids`, atualizados a cada lance pela instrução 22) continuam duplicados em `items` e `items_by_category`, e as duas cópias são atualizadas em toda escrita de lance. A C3 da V8 não isola o atributo volátil, que é o ponto da normalização seletiva de Chen; ela cria mais uma cópia dos estáveis.

### 3.6 Divergências e achados adicionais (relatar antes de continuar)

1. **P = 86 e C = 41 não vêm do catálogo oficial nem da carga do NoSE.** O gerador usa a taxonomia de `design_space_005` (seção 3.4), uma lista própria de 28 consultas numeradas 1–36 (sem as 8 escritas) e uma numeração que não corresponde à do `rubis.rb` (por exemplo, a consulta 1 do gerador é `items_by_id`; a instrução 1 do NoSE lê `users.nickname, users.password`). Não há linhagem de nenhuma tabela de H até C. Na V9, P e C serão gerados do zero; os números da V8 servem só como histórico.
`methodology/design_space_005.py:116`
```python
  116          1: ("items_by_id", ["id"], [], ["id", "name", "description", "initial_price", "quantity", "reserve_price", "buy_now", "nb_of_bids", "max_bid", …
```

2. **O EER da V8 é incompleto para a E3.** `rubis_eer.json` lista 7 entidades e 10 relacionamentos, mas sem atributos, chaves, opcionalidade e cardinalidade por relacionamento. A E3 vai extrair isso do DDL de `rubis_full` e do modelo do NoSE.
3. **Instrução 15 (StoreBuyNow).** O `rubis.rb` declara `UPDATE items SET quantity=?, nb_of_bids=?, end_date=?`, mas a rota e o oráculo da V8 só mudam `quantity` (e regravam `nb_of_bids` sem alterar). A E6 precisa decidir e registrar o contrato (seguir o NoSE ou manter a interpretação da V8); isso afeta quem é volátil (`end_date` é chave de clustering de `items_by_category`, `items_by_region`, `items_by_seller` e `items_by_user_bid`).
`methodology/oracle_011.py:157`
```python
  157      elif statement_id == 15 and item_id in state["items"]:
  158          item = state["items"][item_id]
  159          item["quantity"] = max(0, int(item.get("quantity", 0)) - int(params["qty"]))
```

4. **A coorte não tem arquivo-manifesto com os IDs.** Ela é definida pela regra de derivação (`setup_cohort_a.py`) e pelo conteúdo dos keyspaces `rubis_v8_c008_*`. As contagens serão conferidas contra o enunciado na E17. Na V9, a E17 vai exportar o manifesto (IDs + impressão digital) a partir desses keyspaces antes de materializar.
5. **Commit do QPG.** O commit completo `314e1730175349176b40164086ecb3dc895e41b6` está em `docs/HISTORICAL_SOURCE_PROVENANCE.md`; a cópia local de `inputs/historical_sources/qpg` não tem metadados git, então a fidelidade da E19 será checada por hash dos arquivos de DDL e planos, não pelo commit.
6. **Acesso a disco por mmap** (E1): na Fase 8, limpar o cache do SO não basta; é preciso reiniciar o nó antes de cada repetição, como na medição fria da V8-012 (que já terminou, então a condição da Fase 8 está satisfeita).

## 4. Componentes reaproveitáveis da V8-012 (copiados para `reused/`)

Cópias registradas em `reused/REUSE_MANIFEST.json`: 300 arquivos, 0 modificados depois da cópia.

| Componente | Arquivo copiado | Versão executada |
|---|---|---|
| Oráculo independente | `oracle_011.py`, `oracle_complete_011.py` | `oracle_011.py` idêntico ao snapshot executado em `final_run_v8_completion_011/src/methodology/` (mesmo SHA-256) |
| Protocolo congelado | `protocol_012.py` (`run_protocol`, 4 × 5 × (1.000 + 10.000)) | V8-012 |
| Guarda de isolamento | `protocol_012.py` (`BulkWriterWatch`, `require_idle_node`) | V8-012; adaptado em `schemalens_v9/isolation.py` (D-V9-003) |
| `reset_state` | `benchmark_005.DeploymentAdapter`, `semantic_replay_011.DeploymentAdapter008`, `ablation_factorial_012.FactorialAdapter` | V8-011/012 |
| Aceitação + testes negativos | `acceptance_012.py` (`global_pass_hardcoded = False`) | V8-012 |
| Carregador por streaming | `full_schemalens_012.py` (SchemaLens), `baseline_continuation_011.py` (NoSE/QPG) | V8-012 / V8-011 |
| Supervisor / orquestrador | `supervisor_012.py`, `orchestrator_012.py` | V8-012 |
| Carga | `workloads/rubis.rb` (NoSE `d275634…`), `workload_005.py` (históricos com semente) | V8-005 em diante |

## 5. Estágios E1–E23 no repositório antigo

Classificação: `AUTOMÁTICO` (código gera a saída a partir de entradas), `MANUAL` (valores ou listas escritos à mão), `AUSENTE`, `INVALIDADO POR AUDITORIA`. Versão de referência: `methodology/*_012.py` e `final_run_v8_completion_011/src/methodology/`.

| Estágio | Onde (repositório antigo) | Classificação | Nota |
|---|---|---|---|
| E1 Preflight | `preflight_011.py`, `protocol_012.require_idle_node` | AUTOMÁTICO | na V9: E1 executado |
| E2 Fonte de dados | `dataset/generator`, `historical_loader.py`; `rubis_full` | AUTOMÁTICO | na V9: E2 executado, sem regenerar |
| E3 Esquema conceitual | `engine.conceptual_schema` → `rubis_eer.json` | MANUAL | listas literais de entidades/relacionamentos; sem atributos, chaves, cardinalidades |
| E4 Estatísticas observadas | — | AUSENTE | fan-out vem de palavra-chave, não de `rubis_full` |
| E5 Carga W | `engine.import_workload`, `inputs/workload/rubis_workload.json` | AUTOMÁTICO | SHA do `rubis.rb` registrado |
| E6 Contrato semântico | `oracle_011.py`, `writeplans_011.py` | AUTOMÁTICO (implícito) | sem documento de contrato; instrução 15 diverge do NoSE |
| E7 Catálogo com contratos | `config/catalog_C0_C8.json`, `design_space_005.ARCHITECTURES` | MANUAL | rótulos; duas taxonomias incompatíveis |
| E8 Fragmento e raiz | `engine.analyze` (`ri = entities.split(',')[0]`) | MANUAL | raiz = primeira entidade do texto; não é o Alg. 2 |
| E9 Matriz de evidências | `engine.analyze` | INVALIDADO POR AUDITORIA | por palavra-chave; mecanismos não identificáveis (V8-012) |
| E10 Ativação | `engine.analyze`, `design_space_005` (listas de qid) | MANUAL | qualitativa; limiares não usados |
| E11 Especificações P | `design_space_005.generate_candidate_search_space` | INVALIDADO POR AUDITORIA | P = 86 desligado de H e do catálogo oficial |
| E12 Canonicalização | `design_space_005` (chave arquitetura+PK+CK) | INVALIDADO POR AUDITORIA | C = 41; sem `origin`/`cover`, sem ρmax |
| E13 Síntese de rotas | `benchmark_005.DeploymentAdapter` | MANUAL | rotas escritas à mão por instrução e esquema |
| E14 Modelo de custo | `engine.stage7`, `design_space_005.analytical_metrics` | MANUAL | constantes digitadas; sem calibração |
| E15 Composição H | `final_v7.search`, `design_space_006` | MANUAL | 2 esquemas listados à mão |
| E16 Hbench congelado | `design_space_005:461`, `final_v7.py` | MANUAL | lista fixa |
| E17 Coorte e materialização | `setup_cohort_a.py`, `protocol_012.clone_keyspace` | AUTOMÁTICO | sem manifesto de IDs |
| E18 Corretude | `oracle_011.py`, `semantic_replay_011.py`, `protocol_012.paired_oracle` | AUTOMÁTICO |  |
| E19 Baselines | `baselines_005.py`, `baseline_fidelity_006.py`, `cohort_baselines_012.py` | AUTOMÁTICO | reconstruções |
| E20 Benchmark | `protocol_012.run_protocol` | AUTOMÁTICO |  |
| E21 Análise | `comparison_012.py` (contrastes); `pareto_010.py`, `design_space_006` (Top-k, regret) | AUTOMÁTICO (contrastes) / INVALIDADO POR AUDITORIA (Pareto, regret, Top-k) | métricas retrospectivas da V8-005/006 invalidadas |
| E22 Ablações dos critérios | `ablation_pipeline_011.py`; `ablation_factorial_012.py` | INVALIDADO POR AUDITORIA (011) / AUSENTE (critérios) | 011 copiou keyspaces; o fatorial de 2012 mede tabelas, não critérios |
| E23 Escala completa e aceitação | `full_schemalens_012.py`, `full_cold_012.py`, `acceptance_012.py` | AUTOMÁTICO | medição fria concluída em 11/11 |

## 6. Próxima decisão

Aprovar o início da Fase 1 (E3–E7), considerando as divergências da seção 3.6 — em especial a decisão sobre a instrução 15 (item 3).
