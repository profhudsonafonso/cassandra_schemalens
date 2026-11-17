# Auditoria S1 — janelas temporais (pós-flush B)

- **Estado:** PASS — auditados registros, ordens e estatísticas.
- Medições: **3240** de **3240** previstas.
- Ordens de execução: **1080**.
- Casos × variantes: **54**; todos com oráculo PASS.
- Mesmo `plan_id` físico da seleção B; parâmetros temporais derivados, não nativos Java adicionais.
- Comparação **exploratória** pós-A/B; não utilizar como confirmação independente.
- Latências do S1 **pós-flush**; não comparar valores absolutos diretamente com B pré-flush.

## Q2 e Q12: p95 e chamadas CQL

| Consulta | Janela | TS2-U p95 (ms) | TS2-D p95 (ms) | Chamadas U / D |
|---|---:|---:|---:|---|
| Q2 | 1 d | 1.2294 | 1.2227 | 1 / 1 |
| Q2 | 7 d | 6.8562 | 8.3540 | 1 / 7 |
| Q2 | 14 d | 14.6344 | 21.8299 | 1 / 14 |
| Q2 | 21 d | 19.6050 | 23.4327 | 1 / 21 |
| Q12 | 1 d | 3.6599 | 2.8553 | 2 / 2 |
| Q12 | 7 d | 18.5274 | 19.3199 | 2 / 14 |
| Q12 | 14 d | 28.0670 | 46.4263 | 2 / 28 |
| Q12 | 21 d | 38.1525 | 72.5904 | 2 / 42 |

Contadores de requisições e linhas: executor Python, não métricas internas do servidor Cassandra.
