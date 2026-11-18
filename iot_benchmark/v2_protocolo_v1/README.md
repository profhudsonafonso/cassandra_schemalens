# IoT-Benchmark — versão ajustada (protocolo comum de execução v1)

Esta pasta reexecuta **a fase de medição** do experimento SchemaLens IoT-Benchmark/Cassandra sob o
protocolo comum v1 (`../common/protocol_v1.json`, descrito em `../PROTOCOLO_COMUM_v1.md`). Ela atende às
correções da análise `docs/Analise_Consistencia_Metodologica_Benchmarks.pdf`. A **fase de projeto**
(Stages 1–8 Rev3, campanha B) não é refeita: vem congelada em `frozen_design_B/`, uma cópia byte a byte
da campanha B original, verificada por SHA-256.

O relatório completo está em [`relatorio_overleaf/main.tex`](relatorio_overleaf/main.tex) (e no `main.pdf`).

## O que mudou em relação à execução anterior (Rev3 B / Fases 2–4)

| Item (análise de consistência) | Antes | Versão ajustada (v1) |
|---|---|---|
| Versão do Cassandra (crítico 2) | tag flutuante `cassandra:4.1` (4.1.12), container compartilhado | imagem fixada por digest `sha256:25f6a4d5…` (4.1.12), container **dedicado**, CPUs 0–3, 6 GB, heap 2 GB; cliente nas CPUs 8–11 |
| Métrica e estatística (crítico 3) | p95 nearest rank de 60 amostras, 1 sessão | média, p50, p95 e p99; **métrica de decisão definida antes: média do mix uniforme**; IC 95% por bootstrap estratificado; IC t entre repetições; contrastes pareados; **piso de ruído por pares A/A** |
| Avaliação de preservação (crítico 4) | espaço pequeno demais para isso | **modo verification**: os 6 deployments admissíveis são medidos, e calculam-se Top-1, Top-3, near-best 5%, regret, retenção de Pareto e τ de Kendall |
| Repetições e ordem | 60 rodadas, ordem AB/BA por consulta | **5 repetições com reinício a frio** × (10 aquecimentos + 60 rodadas); janela contínua; ordem de consultas e variantes sorteada e invertida nas rodadas pares |
| Estado físico (importante 11) | gate de flush só no S3 | gate de flush **antes de cada repetição** + **guarda de isolamento** (compactações a cada 15 s + `compaction_history`) + armazenamento por `nodetool tablestats` |
| Escrita e armazenamento (importante 8) | só leitura | tempo de carga e escritas físicas por deployment; bytes de SSTable medidos (objetivos do Pareto) |
| Ablações (importante 10) | nenhuma | cada regra TS0–TS8 / M1–M5 neutralizada e o Stage 8 rodado de novo; mais 5 ablações de critério |
| Aceitação | testes + auditoria Stage 7 × 9 | 10 gates recalculados dos dados brutos + 10 testes negativos |

## Passo a passo

```bash
cd versao_ajustada/iot_benchmark_tsdb
python3 -m venv ../.venv && ../.venv/bin/pip install cassandra-driver numpy matplotlib   # uma vez
./run_all.sh smoke      # opcional: teste rápido em container descartável (não conta como resultado)
./run_all.sh freeze     # 1. pré-registro (PREREGISTRATION_IOT_V1.json, gravação exclusiva)
./run_all.sh measure    # 2. medição v1 -> outputs/v1_run1/ (≈ 15 min)
./run_all.sh analyze    # 3. métricas, bootstrap, piso de ruído, Top-k/regret, figuras
./run_all.sh ablation   # 4. ablações de regras e de critérios (offline)
./run_all.sh accept     # 5. gates + testes negativos -> analysis/acceptance.json
./run_all.sh report     # 6. compila relatorio_overleaf/main.pdf
```

A medição recusa rodar se o código (`iot_v1/`, `../common/`) ou a fase de projeto congelada mudarem depois do
pré-registro. Ela também recusa um container ou um keyspace já existente.

## Estrutura

| Caminho | Conteúdo |
|---|---|
| `frozen_design_B/` | snapshot da campanha B: dados Java, 12 objetos de consulta, Stages 1–8 (V3 → Rev2 → Rev3), código auditado |
| `iot_v1/freeze.py` | verificação do snapshot, variantes, pares A/A, pré-registro |
| `iot_v1/measure.py` | executor v1 (container, carga, flush, repetições, oráculo, guarda de isolamento) |
| `iot_v1/analyze.py` | análise recalculada das latências brutas |
| `iot_v1/ablation.py` | ablações de regras e de critérios |
| `iot_v1/acceptance.py` | gates e testes negativos |
| `PREREGISTRATION_IOT_V1.json` | pré-registro (hashes, variantes, hipóteses H1–H5) |
| `outputs/v1_run1/` | latências brutas, carga, armazenamento, manifesto, `analysis/` |
| `relatorio_overleaf/` | relatório LaTeX (pronto para o Overleaf) e figuras |
