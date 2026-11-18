# TPCx-IoT no Cassandra (adaptação de pesquisa)

Adaptação do TPCx-IoT: ingestão contínua de leituras de sensores concorrendo com a leitura de **duas janelas de 5 s**
(recente e histórica, até 1 800 s antes) por nome de sensor, em todas as estações. **Não é** uma execução certificada do
TPCx-IoT 2.1.2: a hipótese de workload "sensor-name-only / raw_read" vem dos Stages 1–3. O espaço de projeto combina uma
tabela canônica por série (TS0 sem bucket, TS1 com bucket de 60 s ou 3 600 s) com uma projeção opcional por sensor (TS3).

## Volume de dados (campanha medida)

2 estações × 3 sensores, 130 passos de semeadura + 500 passos de 1 s, *padding* de 900 B por evento: **3 780 eventos
ingeridos e 120 leituras de duas janelas por execução** (2 threads de leitura), RF 1. O Stage 7 antigo modelava 10 × 200
sensores, RF 2 e 1 dia; a V3 usa a escala executada.

## Gerações

| Pasta | O que é | Entrada |
|---|---|---|
| [`original/`](original/) | Stages 1–9.1 originais (piloto de pesquisa, harness Stage 9 com *outbox* SQLite) | [`original/README_REPO.md`](original/README_REPO.md) |
| [`v2_protocolo_v1/`](v2_protocolo_v1/) | medição sob o protocolo comum v1; projeto congelado em `frozen_design/` | [`v2_protocolo_v1/README.md`](v2_protocolo_v1/README.md) |
| [`v3_unified/`](v3_unified/) | Stage 7/8 unificado (medição V2 reutilizada) | abaixo |

## Resultados

**V2** (verificação completa: 9 deployments + baseline lógico + 2 réplicas A/A; leitura durante ingestão concorrente):

| deployment | leitura média (ms) | ingestão (eventos/s) |
|---|---|---|
| baseline lógico | 2,857 | 395 |
| TS1-60 + TS3-60 / TS0 + TS3-3600 / TS1-3600 + TS3-3600 / TS1-60 + TS3-3600 / TS0 + TS3-60 / TS1-3600 + TS3-60 | 2,907 – 2,957 (6 empatados) | 240 – 245 |
| TS1-3600 / TS0 / TS1-60 | 4,569 / 4,671 / 5,061 | 377 – 380 |

A projeção TS3 por sensor reduz a leitura de 4 para 2 requisições, mas dobra as escritas: a ingestão cai de ~378 para
~243 eventos/s.

**V3:** espaço de 9; a fronteira = HBench = {TS0, TS0 + TS3-3600}. TS0 + TS3-3600 empata com o melhor (regret 0,17%;
piso A/A 6,13%) e a redução é de 78%. Pontos fracos: o desempate estrutural põe TS0 em primeiro (Top-1 falha) e a
retenção da fronteira medida de três objetivos é 33%, contra 83% na V2. Medições V2 reutilizadas (rotas idênticas).

## Reproduzir

```bash
./reproduce.sh setup && ./reproduce.sh analysis    # refaz V2 e V3 a partir de outputs/v1_run1/raw_reads.csv
./reproduce.sh v2 tpcx_iot                          # nova campanha V2 no Cassandra (~1 h)
```
