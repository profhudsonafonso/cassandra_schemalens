# Versão ajustada dos benchmarks SchemaLens/Cassandra

Correções da análise `docs/Analise_Consistencia_Metodologica_Benchmarks.pdf`. A fase de projeto de cada benchmark
fica congelada; a fase de medição é reexecutada sob o **protocolo comum v1** ([`PROTOCOLO_COMUM_v1.md`](PROTOCOLO_COMUM_v1.md),
[`common/protocol_v1.json`](common/protocol_v1.json)).

| Benchmark | Pasta | Status |
|---|---|---|
| IoT-Benchmark | [`iot_benchmark_tsdb/`](iot_benchmark_tsdb/) | **concluído** (17/11/2026): 28.800 latências, 10/10 gates, 11/11 testes negativos, H1–H5 confirmadas |
| TPCx-IoT | [`tpcx_iot_benchmark/`](tpcx_iot_benchmark/) | **concluído** (17/11/2026): 60 execuções, 7.200 leituras, 12/12 gates, 11/11 testes negativos; H1–H3 confirmadas, H4–H5 rejeitadas |
| TSBS | [`tsbs_tsdb/`](tsbs_tsdb/) | **concluído** (17/11/2026): 7/7 do HBench executados (antes 1/7), dados do gerador oficial Go, oráculo independente, 23.040 latências, 10/10 gates, 10/10 testes negativos; H1, H2, H4, H5 confirmadas, H3 rejeitada |
| RUBiS V9 | [`rubis_v9/`](rubis_v9/) | **concluído** (18/11/2026): E20 com dados regenerados, 4.1.12 × 3.11.11 (ABBA), 3,6 M instruções conferidas pelo oráculo V9 por versão, 12/12 gates, 7/7 testes negativos; H1 e H5 confirmadas, H2–H4 rejeitadas (update100x) |

**Artigo consolidado:** [`artigo_overleaf/`](artigo_overleaf/) (`main.tex`/`main.pdf`), com a metodologia e os quatro experimentos.

`common/` contém o código compartilhado: ambiente Cassandra (container dedicado, preflight, gate de flush, guarda de
isolamento, armazenamento), estatística (bootstrap, contrastes pareados, piso de ruído A/A) e avaliação do método
(Top-k, regret, retenção de Pareto, τ de Kendall). Ambiente Python: `python3 -m venv .venv && .venv/bin/pip install cassandra-driver numpy matplotlib`.
