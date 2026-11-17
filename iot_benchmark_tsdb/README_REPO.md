# SchemaLens–Cassandra para séries temporais IoT (IoT Benchmark)

Experimento de adaptação do SchemaLens ao Apache Cassandra com cargas do
[IoT Benchmark](https://github.com/thulab/iot-benchmark) (commit `62e0dc100e56443978c8b4c66472b1d8a7ef0238`).
Período: 16/10/2026 a 12/11/2026.

**Comece por aqui:** [`docs/relatorio_overleaf/main.pdf`](docs/relatorio_overleaf/main.pdf) — relatório
metodológico completo (fonte LaTeX em `docs/relatorio_overleaf/main.tex`, pronta para o Overleaf).

## Estrutura

| Pasta / arquivo | Conteúdo |
|---|---|
| `phase_a1/` | Stages 1–3: evidência e ativação das famílias TS0–TS8 (regras vendorizadas com hash) |
| `iot_pipeline/` | Stages 4–9 da V3 (candidatos, planos, proxies, HBench, executor) |
| `config/pipeline_policy.json` | Política V3 (pesos, `top_k`, variantes TS2) |
| `native_auto_work/runs/` | Execução V3 nativa (dados Java, Stage 9, diagnósticos E1–E3, V4) |
| `SchemaLens_V4_*`, `SchemaLens_Rev2_*`, `SchemaLens_Rev3_*` | Pacotes de cada revisão, correção e auditoria |
| `experiments/rev3_s2_partition_growth/` | S1 (casos de janela) e S2 (densidade) |
| `experiments/rev3_s3_physical_storage/` | S3 R1 (estado físico controlado) |
| `experiments/rev3_fase2/` | Fases 2–4 (E0–E18): protocolo, emendas 01–05, pré-registros, latências brutas, auditorias |
| `*.zip` | Pacotes de resultados auditados de cada etapa |

## O que não está no repositório

- Clone do upstream e build Maven (`native_auto_work/upstream_iot_benchmark/`): regenerar com `./run_v3_auto.sh`.
- CSVs de dados gerados (`experiments/rev3_fase2/data/**/*.csv`, `experiments/rev3_s2_partition_growth/generated/*.csv`,
  ~260 MB): reprodutíveis pelas sementes; os SHA-256 estão nos `manifest.json`.

O `README.md` desta pasta é o README original da ponte V3.
