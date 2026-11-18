# TSBS cpu-only no Cassandra

Caso de uso `cpu-only` (DevOps) do [Time Series Benchmark Suite](https://github.com/timescale/tsbs): 16 *templates* de
consulta (single-groupby, double-groupby, cpu-max-all, high-cpu, lastpoint, groupby-orderby-limit). São aplicadas
apenas as regras de séries temporais TS0–TS8 e os modificadores M1–M5. O espaço combina a tabela bruta por série (TS0)
com tabelas opcionais: larga com bucket diário (TS2), *rollups* por minuto/hora (TS5) e dimensão por hora (TS6).

## Volume de dados (campanha medida)

Gerador Go oficial `tsbs_generate_data` (commit `8323e59c`), `--use-case=cpu-only --scale=40 --log-interval=600s`,
36 h a partir de 2020-01-01, semente 20261117: **40 hosts, 8 640 observações, 86 400 células métricas**
(`v2_protocolo_v1/data/`, com os dois SHA-256 no pré-registro). São 96 instâncias de consulta congeladas: 16 *templates*
× 3 × duas coortes, *natural* (janelas arbitrárias, decisão) e *alinhada* (janelas alinhadas às tabelas de *rollup*).
O Stage 7 antigo modelava 100 hosts, 60 s e 3 dias; a V3 usa a escala executada.

## Gerações

| Pasta | O que é | Entrada |
|---|---|---|
| [`original/`](original/) | ativação TS-only e Stages 4–9 originais (1 de 7 deployments executado no Stage 9) | [`original/README.md`](original/README.md) |
| [`v2_protocolo_v1/`](v2_protocolo_v1/) | dados do gerador oficial, oráculo independente, medição v1 (HBench + Pareto + baseline + A/A) | [`v2_protocolo_v1/README.md`](v2_protocolo_v1/README.md) |
| [`v3_unified/`](v3_unified/) | Stage 7/8 unificado e **nova campanha**: verificação completa dos 24 deployments V3 | abaixo |

## Resultados

**V2** (12 variantes; média do mix de 16 *templates*, coorte natural): as coberturas SchemaLens ficam entre 75,2 e
77,9 ms, contra **129,6 ms** do esquema original do TSBS (`series_bigint`).

**V3** (26 variantes × 5 repetições a frio; 49 920 consultas cronometradas, todas conferidas pelo oráculo):

| | coorte natural | coorte alinhada |
|---|---|---|
| piso A/A | 12,3% (23 de 24 empatam com o melhor) | 4,9% |
| melhor medido | TS0+TS2+TS5gm+TS5hh (72,4 ms, fora do HBench) | TS0+TS2+TS5gm+TS6 (48,2 ms, **no HBench**) |
| HBench V3 (5 de 24) | regret 1,7%, Top-1 no ruído, retenção 33% | regret 0, retenção 57% |
| rank 1 V3 / esquema original do TSBS | **0,548** [0,529; 0,568] | **0,379** |

Por que medir de novo: em dois deployments do HBench, a regra de rota universal manda 5 instâncias alinhadas para TS0
em vez de TS5hh, combinação que a V2 nunca mediu (D-V3-008).

## Reproduzir

```bash
./reproduce.sh setup && ./reproduce.sh analysis    # refaz V2 e V3 a partir das latências brutas versionadas
./reproduce.sh v2 tsbs                              # gera os dados (Docker golang:1.22.5) e mede a V2 (~45 min)
./reproduce.sh v3                                   # campanha V3 (TSBS ~1,5 h, depois RUBiS)
```

Arquivos V3: `v3_unified/prebenchmark/` (vetores, Pareto, HBench, `FROZEN.sha256`), `v3_unified/PREREGISTRATION_V3.json`,
`v3_unified/benchmark/run1/` (latências brutas, carga, armazenamento, manifesto) e `v3_unified/analysis/`.
