# SchemaLens Rev2 — Auditoria dos Stages 7 e 8

**Estado:** AUDITORIA PARCIAL (sem o congelamento Stage7/8)

- Medições brutas verificadas: **2400**.
- Ordens de execução verificadas: **480**.
- p95/p99 e médias por consulta recalculados: **60**.
- Unidade da latência: **milissegundos ponta a ponta no cliente** (driver + processamento Python).
- Agregado: **média não ponderada de p95 por consulta**, NÃO p95 da mistura do workload.
- Q1 tem zero resultados segundo o manifesto; não interpreta como carga útil positiva.

## Agregados observados

| Variante | Média dos p95 Q1–Q12 (ms) | Sem Q8 (ms) | Sem Q5 (ms) |
|---|---:|---:|---:|
| `BASELINE_Q8_OPTIMIZED` | 2.3420 | 2.5086 | 1.8885 |
| `R2H_e72ef4d8529d` | 2.6729 | 2.8700 | 1.8974 |
| `BASELINE_ORIGINAL` | 3.8577 | 3.1055 | 2.9565 |
| `R2H_bdc993acdde3` | 4.0364 | 4.3557 | 2.5945 |
| `R2H_6355f4e57096` | 4.5154 | 4.1109 | 3.5688 |

## Plano físico: previsão versus observação

**PENDENTE:** para calcular a concordância das estimativas do Stage 7, execute novamente este auditor com `--experiment` apontando ao experimento confirmatório que contém `outputs/v4_rev2_prebenchmark/`. Os CSVs enviados registram as operações reais, mas não incluem os proxies por plano congelados.

Em especial, o executor TS0 (`TS0_FROZEN_ROUTE`) registrou campos `requests` e `iterated_rows` vazios; esse instrumento precisa ser acrescentado em uma campanha *nova* para comparar suas estimativas de acesso.

## Limites e decisões metodológicas

1. **Nenhum resultado daqui deve ser retroalimentado no mesmo HBench confirmatório.** Alterar proxies exige uma nova revisão e outro holdout.
2. A comparação de `requests` e `iterated_rows` é frente às operações registradas pelo executor Python; não são contadores internos de *server-side read* do Cassandra.
3. Os bytes transferidos e tamanhos reais de SSTables/partições devem ser avaliados numa etapa separada, sem alterar este congelamento.
4. TS4 pressupõe *batch snapshot*; atualizações concorrentes não foram avaliadas.
5. Este procedimento audita a consistência dos arquivos de seleção e do benchmark; não é reprodução oficial de SQL/IoTDB ou mix de frequências.

## Próxima etapa depois desta auditoria

Instrumentar TS0 para contar requisições/linhas, medir armazenamento real por tabela e tamanho/partições no Cassandra, repetir em uma nova campanha e comparar erros dos proxies por classe de consulta.
