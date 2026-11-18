# IoT-Benchmark no Cassandra

Workload de séries temporais do [IoT Benchmark](https://github.com/thulab/iot-benchmark) (commit `62e0dc10`, modelo
*table* do IoTDB): 12 consultas (Q1–Q12: janelas, agregações, *group by*, último valor, …). A metodologia ativa as
famílias de séries temporais TS0–TS8 e os modificadores M1–M5 e escolhe entre tabela estreita por série (TS0), tabela
larga por dispositivo com ou sem bucket diário (TS2-U / TS2-D) e tabela de último valor (TS4).

## Volume de dados (campanha B, a medida)

9 dispositivos, 54 000 células métricas (`frozen_design_B/phase_a1/outputs/normalized_double_readings.csv`), 12 objetos
de consulta gerados pelo código Java oficial (`query_instances_pilot.json`), janela de consulta de 7 dias.
Semente de dados 531101; semente de consultas 531107.

## Gerações

| Pasta | O que é | Entrada |
|---|---|---|
| [`original/`](original/) | experimento original, Rev2 → Rev3 *plan-aware* → V4, ponte oficial dos objetos de consulta Java | [`original/README_REPO.md`](original/README_REPO.md), relatório em `original/docs/relatorio_overleaf/` |
| [`v2_protocolo_v1/`](v2_protocolo_v1/) | medição refeita sob o protocolo comum v1; projeto da campanha B congelado em `frozen_design_B/` | [`v2_protocolo_v1/README.md`](v2_protocolo_v1/README.md) |
| [`v3_unified/`](v3_unified/) | Stage 7/8 unificado: pré-benchmark, freeze, pré-registro e análise (medição V2 reutilizada) | abaixo |

## Resultados

**V2** (verificação completa: 6 deployments + 2 baselines + par A/A; 5 repetições; média do mix uniforme Q1–Q12):

| deployment | média (ms) |
|---|---|
| REF-Q8opt (referência, Q8 otimizada) | 4,117 |
| **TS2-U** (rank 1 da V2) | **4,145** (mesmo DDL da referência, empate A/A) |
| TS2-U + TS4 | 4,158 |
| REF (referência original) | 4,971 |
| TS0 / TS0 + TS4 | 5,455 / 5,462 |
| TS2-D / TS2-D + TS4 | 6,924 / 6,906 |

**V3:** o espaço viável tem 3 deployments; TS4 é tabela morta pela regra estrutural, porque a rota lexicográfica não a
usa. A fronteira de Pareto tem um ponto só, e o HBench V3 = {TS2-U} é o melhor medido: regret 0, retenção de Pareto
100%, redução de 67%. Como TS2-U, TS0 e TS2-D foram medidos na V2 com rotas idênticas, a V3 reutiliza essas medições
(D-V3-001). Piso A/A: 2,15%.

Arquivos: `v3_unified/prebenchmark/09_hbench_v3.json`, `v3_unified/analysis/metrics.json`,
`v3_unified/analysis/v2_vs_v3_results.csv`.

## Reproduzir

Na raiz do repositório:

```bash
./reproduce.sh setup && ./reproduce.sh analysis    # refaz as análises V2 e V3 a partir de outputs/v1_run1/raw_latencies.csv
./reproduce.sh v2 iot                               # nova campanha V2 no Cassandra (~1 h), em outputs/<RUN>
```

O IoT da V3 não tem harness de medição próprio: o HBench V3 está contido no espaço medido pela V2
(`v3_unified/benchmark/REUSED_FROM_V2.json`, com os SHA-256 dos arquivos brutos reutilizados).
