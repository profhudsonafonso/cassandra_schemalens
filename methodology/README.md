# Metodologia (comum aos quatro benchmarks)

| Pasta | Conteúdo |
|---|---|
| [`protocolo_v1/`](protocolo_v1/) | **Protocolo comum de execução v1** ([`PROTOCOLO_COMUM_v1.md`](protocolo_v1/PROTOCOLO_COMUM_v1.md), `protocol_v1.json`). `cassandra_env.py`: contêiner dedicado por digest, reinício a frio, gate de flush, guarda de isolamento, tablestats. `slstats.py`: posto exato, bootstrap pareado estratificado, piso A/A, vereditos. `method_eval.py`: Top-k, regret, retenção de Pareto, Kendall τ, k aleatório. |
| [`unified_stage7_v3/`](unified_stage7_v3/) | **Stage 7/8 unificado (V3).** Modelo canônico (`model.py`), proxy de bytes LBP-1, estimador de trabalho físico, agregação e viabilidade, Pareto, diversidade e `balanced_structural_tiebreak`, firewall (`guards.py`), freeze/pré-registro (`freeze.py`, `plans.py`), adaptadores por benchmark, harnesses Stage 9 (`stage9/`), análise retrospectiva (`analysis/`), 15 testes obrigatórios (`tests/`), mapa de caminhos (`layout.py`) e atestação de relocação (`relocation.py`). |
| [`docs/`](docs/) | [Análise de consistência metodológica](docs/Analise_Consistencia_Metodologica_Benchmarks.pdf), que motivou a V2. `README_versao_ajustada_V2.md` (visão geral da V2). [`v3/`](docs/v3/): método, auditoria do Stage 7 V2, comparação pré-benchmark, resultados, limitações, *decision log* e logs da campanha. |
| [`artigos/`](artigos/) | Artigos Overleaf: [`v2_protocolo_v1/`](artigos/v2_protocolo_v1/main.pdf) (metodologia + os quatro benchmarks sob o protocolo v1) e [`v3_unified/`](artigos/v3_unified/main.pdf) (Stage 7/8 unificado). Tabelas e figuras geradas por `build_tables.py` a partir dos resultados. |
| `run_v3_unified.sh` | Execução da V3 por fases: `--audit --prebenchmark --test --freeze --smoke --benchmark --status --analyze` |
| `reproduce_check.py` | Verificação offline usada por `../reproduce.sh analysis`: refaz as análises V2/V3 a partir das latências brutas e compara. |

## Fases do SchemaLens

```
workload + esquema conceitual
  → ativação de famílias (regras declarativas)            Stages 1–3
  → candidatos físicos (CandidateSpec)                     Stage 4
  → rotas de leitura (QueryPlan) / escrita (WritePlan)     Stages 5–6
  → estimativa de trabalho físico (vetor, sem latência)    Stage 7  (V3: unified_stage7_v3)
  → deployments completos, Pareto, diversidade → HBench    Stage 8
  ── freeze por SHA-256 + pré-registro ── firewall ──
  → medição no Cassandra sob o protocolo comum v1          Stage 9
  → preservação, regret, ablações, contrastes              análise
```

## Rodar a V3 manualmente

Do diretório `methodology/`, com o ambiente da raiz (`../setup_env.sh`):

```bash
../.venv/bin/python -m unified_stage7_v3.run_prebenchmark tsbs --out /tmp/pre   # Stage 7/8 de um benchmark
../.venv/bin/python -m pytest unified_stage7_v3/tests -q                       # 15 testes obrigatórios
../.venv/bin/python -m unified_stage7_v3.freeze verify rubis                   # verifica o freeze (ABORT se mudou)
../.venv/bin/python -m unified_stage7_v3.relocation check                      # refaz a atestação de relocação
../.venv/bin/python -m unified_stage7_v3.analysis.run_all                      # análise V3 -> <benchmark>/v3_unified/analysis
```

O pacote é importado como `unified_stage7_v3` a partir de `methodology/`. Todos os caminhos do repositório ficam em
`unified_stage7_v3/layout.py`.
