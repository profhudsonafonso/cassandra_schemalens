# Versão ajustada dos benchmarks SchemaLens/Cassandra

Correções da análise `docs/Analise_Consistencia_Metodologica_Benchmarks.pdf`. A fase de projeto de cada benchmark
fica congelada; a fase de medição é reexecutada sob o **protocolo comum v1** ([`PROTOCOLO_COMUM_v1.md`](PROTOCOLO_COMUM_v1.md),
[`common/protocol_v1.json`](common/protocol_v1.json)).

| Benchmark | Pasta | Status |
|---|---|---|
| IoT-Benchmark | [`iot_benchmark_tsdb/`](iot_benchmark_tsdb/) | **concluído** (17/11/2026): 28.800 latências, 10/10 gates, 11/11 testes negativos, H1–H5 confirmadas |
| TPCx-IoT | [`tpcx_iot_benchmark/`](tpcx_iot_benchmark/) | **concluído** (17/11/2026): 60 execuções, 7.200 leituras, 12/12 gates, 11/11 testes negativos; H1–H3 confirmadas, H4–H5 rejeitadas |
| TSBS | [`tsbs_tsdb/`](tsbs_tsdb/) | em execução |
| RUBiS | — | fora de escopo nesta rodada |

`common/` contém o código compartilhado: ambiente Cassandra (container dedicado, preflight, gate de flush, guarda de
isolamento, armazenamento), estatística (bootstrap, contrastes pareados, piso de ruído A/A) e avaliação do método
(Top-k, regret, retenção de Pareto, τ de Kendall). Ambiente Python: `python3 -m venv .venv && .venv/bin/pip install cassandra-driver numpy matplotlib`.
