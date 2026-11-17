# Explicação da adaptação TSBS ao SchemaLens — somente TS0–TS8

## O que foi preservado

O código do ZIP original SmartRetail foi reutilizado para o motor `activate`/`evaluate_rule` e para o construtor de features temporais. O arquivo `config/tsdb_activation_rules.json` não foi reescrito e todas as 9 famílias TS0–TS8 são avaliadas para cada consulta; o mesmo vale para os 5 modificadores M1–M5. Não há execução de C0–C9.

## Evidências e exemplo

A consulta `high-cpu-1` recupera dez métricas de CPU por host e por intervalo temporal e aplica `usage_user > 90`. O construtor calcula:

- `is_temporal_query=true` a partir do manifesto TEMPORAL + `CPUReading.temporal` no esquema conceitual;
- `series_identity_bound=true` porque a consulta filtra uma série identificada por `hostname`;
- `returns_raw_observations=true` porque projeta métricas sem agregação;
- `has_time_range=true` por `[start,end)`;
- `metric_count=10` por contar as dez métricas projetadas.

Passam então TS0, TS1 e TS2. No cenário default M1 passa porque TS1 está ativa; M3 passa por `append_oriented=true`. Não se presume retenção de 730 dias ou backfill, portanto M2 e M5 não são ativados.

Para `double-groupby-*`, a consulta utiliza `TIME_BUCKET('1 hour', event_ts)`, AVG e agrupa também por hostname. As regras ativam TS5 (agregado temporal) e TS6 (agregado por dimensão não temporal). TS0 continua como alternativa bruta para comparação.

## Atenção a `lastpoint`

`lastpoint` significa **última observação por host**. A regra TS4 herdada do SmartRetail exige `ORDER BY time DESC LIMIT 1`; consequentemente esta workload não a ativa. Isso é uma lacuna de cobertura sintática/semântica a estudar depois, sem modificar o resultado agora.

## Artefatos para supervisores

1. `inputs/schema/tsbs_cpu_only_schema.json`: entidades e metadados temporais usados como entradas (EER adaptado, não distribuído pelo TSBS).
2. `inputs/workload/query_manifest.csv`: 16 templates registrados no gerador TSBS.
3. `inputs/workload/sql/`: SQL lógico de cada template (não CQL executável).
4. `outputs/tsbs_run/02_combined_evidence.csv`: features por consulta.
5. `outputs/tsbs_run/04_tsdb_activation_matrix.csv`: famílias ativadas.
6. `outputs/tsbs_run/05_temporal_modifier_matrix.csv`: M1–M5.
7. `outputs/tsbs_run/06_full_activation_trace.json`: PASS/FAIL regra a regra e condição a condição.
8. `outputs/tsbs_run/09_run_manifest.json`: contagens e ressalvas.

## Reprodutibilidade

Do terminal do VS Code:

```bash
./run_tsbs_tsdb_only.sh
python3 -m unittest discover -s tests -v
```

Esta etapa **não** executa Cassandra nem contém evidência empírica de latência. O próximo desenvolvimento será converter famílias temporais ativadas em `CassandraCandidateSpec`, incluindo alternativas PK/CK/CQL, cobertura e manutenção, antes de comparar por TSBS.
