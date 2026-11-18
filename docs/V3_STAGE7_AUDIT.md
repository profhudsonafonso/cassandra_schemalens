# V3 — Auditoria do Stage 7 / Stage 8 antes da padronização

Data: 2026-11-18. Branch `experiment/unified-stage7-v3`, criada a partir de `versao-ajustada-tsdb` @ `6ce40fd`.
Ambiente: Python 3.12.3; Docker 29.3.0; Cassandra da campanha anterior (V2 = `versao_ajustada/`, protocolo comum v1):
`cassandra@sha256:25f6a4d5c258fa7295cb4f4ccd7d94a21e565a1040ba1c205455fc11ff8041fc` (release 4.1.12); controle RUBiS
3.11.11 `cassandra@sha256:5e13c93dfc49c87dda562e2450b79e2d6f262f0b675ba90bdb382bbaf91bcd33`.

Nomenclatura: **V1** = experimentos originais de cada benchmark; **V2** = `versao_ajustada/` (protocolo comum de
execução v1, mesma seleção de projeto de cada benchmark); **V3** = este trabalho (Stage 7/8 unificados).

## 1. Tabela comparativa

| | IoT-Benchmark | TPCx-IoT | TSBS cpu-only | RUBiS |
|---|---|---|---|---|
| **Stage 7 que gerou o HBench medido na V2** | Rev2/Rev3 *plan-aware*: `SchemaLens_V4_Rev2_Confirmatory/v4_rev2/cost.py` + `SchemaLens_V4_Rev3_PlanAware/rev3/core.py` (o `iot_pipeline/stage7.py` pedido é a V3 original, NÃO usada no HBench da V2 — ver D-V3-002) | `methodology/tpcx_iot/stage7/estimator.py` | `methodology/tsbs/stage7/estimator.py` + `operators.py` | E14 `schemalens_v9/cost.py` → `COST_MODEL.json` |
| **Inputs** | dados Java da campanha B (54.000 células), 12 objetos de consulta, specs Rev2, cobertura cruzada | specs/QueryPlans/WritePlans dos Stages 4–6 + cenário ilustrativo da política | specs/planos dos Stages 4–6 + cenário `inputs/schema/tsbs_cpu_only_schema.json` | E3 EER, E4 estatísticas, E5 frequências, E13 rotas, **latências brutas da V8-012** |
| **Proxies calculados** | requisições, linhas físicas, KiB retornados, fan-out extra de buckets, pressão de partição; armazenamento e escrita por candidato | 5 eixos: requisições CQL por leitura, armazenamento lógico replicado, inserts por evento, bytes máximos de partição, nº de partições | leitura (requisições, linhas), armazenamento, chaves de escrita tocadas, warnings | reads, rows (por rota), writes, support reads |
| **Uso de latência** | não (coeficientes declarados) | não | não | **sim**: NNLS ajustado em latências da V8-012 (a=0,462 ms/read, b=0,389 ms/write, c=0,0075 ms/row; R²=0,999) |
| **Pesos / escalarização** | leitura = 4·req + 0,02·linhas + 0,20·KiB + 2·fan-out + 5·pressão; seleção 0,8/0,1/0,1 (leitura/armazenamento/escrita); a V3 original tinha fatores por família TS0 1,0 / TS1 0,85 / TS2 0,62 / TS4 0,12 / TS5 0,15 | 0,45/0,15/0,20/0,15/0,05 normalizados pelo controle TS0 | leitura 0,2·req + 0,8·linhas; 0,8/0,1/0,1 | objetivo ILP em ms: Σf·(a·reads + c·rows) + Σf·(b·writes + (a+c)·support) |
| **Escala do modelo** | a mesma dos dados medidos (campanha B) | **10 estações × 200 sensores, 60 s, 1 dia, RF=2** (ilustrativo) | **100 hosts, 60 s, 3 dias** | **escala completa**: 2 M itens, 20 M lances (E02/E04) |
| **Escala executada (V2)** | campanha B (9 disp., 54.000 células) | 2 estações × 3 sensores, 1 s, 630 passos, RF=1 | 40 hosts, 600 s, 36 h | coorte regenerada: 7.392 itens, 5.690 lances (servidor) |
| **Stage 8** | `v4_auto/selector.search_covers` + `rev3/core.select_schema_and_plan` | `stage8/consolidator.py` | `stage8/consolidator.py` | E15 `compose.py` (ILP PuLP/CBC) |
| **Pareto** | 3 eixos (leitura, armazenamento, escrita) | 5 eixos estruturais | 3 eixos normalizados, por perfil | ILP com somas ponderadas numa grade de pesos, mix bidding |
| **Seleção** | top_k=5 por escore 0,8/0,1/0,1 + diversidade de fonte canônica | controle TS0 obrigatório + novidade de tabelas, top_k=5 | top_k=5 por perfil, por escore (sem diversidade) | ótimo por carga + Pareto + k-best até |H|=5 |
| **HBench atual (V2)** | {TS2-U, TS0, TS2-D} (3 de 6) | 5 de 9 (TS0; TS1h+TS3h; TS1m+TS3m; TS0+TS3h; TS1h) | 7 (2 conservative + 5 conditional_aligned) de 26 | H01–H05 |
| **Arquivos** | `iot_pipeline/stage7.py`, `stage8.py`, `config/pipeline_policy.json`; Rev2/Rev3 acima | `stage7/estimator.py`, `stage8/consolidator.py`, `config/tpcx_stage7_cost_policy.json`, `config/tpcx_stage8_consolidation_policy.json` | `stage7/estimator.py`, `operators.py`, `stage8/consolidator.py`, `config/tsbs_stage7_cost_policy.json`, `config/tsbs_stage8_consolidation_policy.json` | `schemalens_v9/cost.py`, `compose.py`, `routes.py`, `phase2.py`, `catalog.yaml`, `runs/.../E14_cost_model/COST_MODEL.json` |

## 2. Discrepâncias encontradas (registradas em `V3_DECISION_LOG.md`)

1. **IoT**: os arquivos `iot_pipeline/stage7.py`/`stage8.py` não produziram o HBench da V2. O HBench veio do Rev3
   *plan-aware* (Rev2 + Rev3), com coeficientes e regra de seleção diferentes. As duas implementações coexistem.
2. **TPCx-IoT**: o Stage 7 modela 10 estações × 200 sensores com RF=2, mas a execução usa 2 × 3 com RF=1. As
   requisições CQL previstas (20/30/3) não correspondem às observadas (4/4–6/2–3); a V2 rejeitou a H5 por isso.
3. **TSBS**: o Stage 7 usa 100 hosts / 60 s / 3 dias; a campanha V2 usa 40 hosts / 600 s / 36 h do gerador oficial.
4. **RUBiS**: as rotas e o custo usam estatísticas de **escala completa** (10 lances por item, 4.000 itens por
   categoria...), mas o E20 sempre mediu uma **coorte** (V9: ≈6 mil linhas por entidade). Isso viola o requisito 9
   também na V9 original, não só na V2.
5. **RUBiS**: a seleção depende de coeficientes NNLS ajustados em latências da V8 (não circular dentro da V9, mas
   proibido na V3).
6. Os quatro Stage 8 usam regras de Pareto, eixos, top-k e diversidade diferentes.
7. O remoto (`origin`) só tem `main` @ `acef055`; os commits da V2 existem só localmente (o push falhou por
   autenticação). O `pull` não se aplica à branch de trabalho.
