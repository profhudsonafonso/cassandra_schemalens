# TPCx-IoT — versão ajustada (protocolo comum de execução v1)

Reexecução da **fase de medição** do experimento SchemaLens TPCx-IoT/Cassandra sob o protocolo comum v1
(`../PROTOCOLO_COMUM_v1.md`). A fase de projeto (Stages 1–8) vem congelada em `frozen_design/`: uma cópia byte a
byte de `tpcx_iot_benchmark/` (109 arquivos verificados por SHA-256), validada também pelo próprio `load_frozen()`
do código congelado.

Relatório: [`relatorio_overleaf/main.tex`](relatorio_overleaf/main.tex) (e `main.pdf`).

## O que mudou em relação à Etapa 9.1

| Item | Etapa 9.1 | v1 |
|---|---|---|
| Cassandra | compose 4.1.8 × execução 4.1.12, container compartilhado | 4.1.12 por digest, container dedicado `sl_v1_tpcx_cassandra`, CPUs 0–3 |
| Repetições | 3, rotação, `DROP`/reset por execução | 5 com reinício a frio, ordem contrabalançada por par de repetições, keyspace **novo** por execução (sem `DROP`) |
| Estatística | mediana do p95 por repetição | média de leitura (decisão), p50/p95/p99, IC bootstrap pareado pela agenda, IC t de ingestão/disco, piso de ruído por **réplicas A/A** |
| Estado físico | flush só para armazenamento | semente + flush + **gate** antes da fase dinâmica; guarda de isolamento; `tablestats` |
| Preservação | Top-k e Pareto 2D | Top-k, regret, Pareto **3D** e 2D, τ de Kendall, aleatório k=5 |
| Ablações | nenhuma | 14 regras neutralizadas (Stages 1–4 regenerados) + 5 critérios |
| Aceitação | 142 testes unitários | 12 gates dos dados brutos + 11 testes negativos |

A carga do TPCx não foi alterada: 2 estações × 3 sensores, 130 + 500 passos, 120 leituras de duas janelas em 2 threads, concorrentes com o writer.

## Passo a passo

```bash
cd versao_ajustada/tpcx_iot_benchmark
./run_all.sh smoke      # opcional (container descartável)
./run_all.sh freeze     # PREREGISTRATION_TPCX_V1.json (exclusivo)
./run_all.sh measure    # ~22 min -> outputs/v1_run1/
./run_all.sh analyze
./run_all.sh ablation
./run_all.sh accept
../.venv/bin/python tpcx_v1/make_tables.py --run outputs/v1_run1
./run_all.sh report
```

## Principais resultados (v1)

- Os deployments que leem de uma projeção TS3 leem ≈37% mais rápido, mas ingerem ≈36% mais devagar e ocupam ≈2× o disco.
- O HBench contém o melhor medido (regret 0, Pareto 2D 100%, Pareto 3D 83%). O escore do seletor, porém, não prediz a leitura (τ = −0,06): a preservação vem da diversidade.
- O baseline lógico do TPC (uma tabela por `sensor_name`) empata em leitura com os melhores deployments e é melhor em ingestão e disco.
- 12/12 gates e 11/11 testes negativos passaram; H1–H3 foram confirmadas e H4–H5 rejeitadas, como registrado.
