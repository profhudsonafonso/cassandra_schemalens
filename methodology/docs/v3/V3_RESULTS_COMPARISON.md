# V3 — Resultados e comparação V2 × V3

Gerado por `python -m unified_stage7_v3.analysis.run_all` **depois** das medições; o freeze de cada benchmark é verificado antes. Kendall τ é diagnóstico retrospectivo. O modelo NNLS legado aparece só como `RETROSPECTIVE_BASELINE_ONLY`.

## Resumo e discussão (escrita depois da medição; não realimenta o Stage 7/8)

### Quadro V2 × V3 (decisão: média; ruído pelo piso A/A)

| benchmark | método | HBench / espaço | DSR | Top-1 (ruído) | Top-3 | regret | retenção Pareto | τ (diag.) |
|---|---|---|---|---|---|---|---|---|
| IoT | V2 (relatado; espaço V2 = 6) | 3/6 | 0,50 | sim | sim | 0 | 1,00 | 0,87 |
| IoT | V2 no denominador V3 | 3/3 | 0 | sim | sim | 0 | 1,00 | 1,00 |
| IoT | **V3** | **1/3** | **0,67** | **sim** | sim | **0** | **1,00** | 1,00 |
| TPCx | V2 (= denominador V3) | 5/9 | 0,44 | não | sim | 0 | 0,83 | −0,06 |
| TPCx | **V3** | **2/9** | **0,78** | **não** | sim | **0,0017** | **0,33** | −0,34 |
| TSBS natural | V2 (campanha V2, espaço medido 10) | 7/10 | 0,30 | sim | sim | 0,0017 | 0,25 | −0,09 |
| TSBS natural | **V3** (campanha V3, 24 medidos) | **5/24** | **0,79** | **sim** | sim | **0,0167** | **0,33** | 0,35 |
| TSBS aligned | V2 | 7/10 | 0,30 | não | sim | 0 | 0,67 | 0,54 |
| TSBS aligned | **V3** | **5/24** | **0,79** | **sim** | sim | **0** | **0,57** | 0,36 |
| RUBiS 4.1 (dentro de H) | V2 (NNLS, H01–H05) | 5/— | — | 3 de 4 cargas | — | 0 a 0,19 ms | — | 0,2–1,0 |
| RUBiS 4.1 (dentro de H) | **V3** (V3H01–V3H05) | **5/256 viáveis** | **0,98** | **4 de 4 cargas** | — | 0 (3 cargas), 13,8% em update100x (empate pelo piso de 23%) | — | −0,6 a 0,8 |

Os denominadores são diferentes e estão declarados. No TSBS a V2 mediu 10 coberturas e a V3 mediu as 24 do espaço V3
(verificação completa). No RUBiS só o HBench e os baselines foram medidos.

### Qualidade de preservação (pergunta central)

* **IoT:** o único deployment retido é o melhor medido. Preservação total com redução de 3 para 1.
* **TPCx:** o HBench V3 (2 de 9) contém `h_3f1449e05468`, estatisticamente empatado com o melhor (regret de 0,17%).
  A preservação do melhor nível vale. Já a retenção da fronteira medida (leitura × armazenamento × escrita) cai para
  1/3, porque a fronteira estrutural V3 só tem 2 pontos e 6 deployments empatam na leitura.
* **TSBS:** na coorte natural, com piso A/A de 12,3%, 23 dos 24 deployments empatam com o melhor. O HBench (5 de 24)
  contém empatados (regret de 1,7%). Na coorte alinhada, onde as tabelas TS5/TS6 servem, o melhor medido está no HBench
  (regret 0). Isso preserva o melhor com 79% de redução.
* **RUBiS:** em 4.1.12 o melhor HBench V3 é mais rápido que NoSE e QPG em browsing, bidding e update10x
  (R1 = sim; razões de 0,71 a 0,77). Em update100x tudo empata pelo piso. Em 3.11.11 os vereditos são os mesmos
  (R2 = sim). Oráculo: 3 577 311 / 3 577 311 respostas iguais em cada versão. **Achado negativo:** `V3H05`, admitido
  pela regra de diversidade estrutural (tags C005/C008/C013/C015), é 1,2× a 2,9× mais lento que os baselines nas
  três primeiras cargas. A diversidade preservou variedade estrutural, não desempenho.

### Qualidade de ranking (diagnóstico)

O `balanced_structural_tiebreak` não é preditor. O τ vai de −0,34 (TPCx) a 1,0 (IoT) e fica em torno de 0,35 no TSBS.
No RUBiS 4.1 o τ é 0,8 em browsing, bidding e update10x e −0,6 em update100x. O NNLS legado,
**RETROSPECTIVE_BASELINE_ONLY**, aplicado depois da campanha ao HBench V3, tem τ semelhante (0,8–0,84; −0,2 em
update100x). Ele acerta o Top-1 em 3.11 bidding, mas erra o Top-1 em 3.11 update100x, onde o V3 acerta. Nenhum dos
dois domina o outro, e o V3 não usa coeficientes de latência.

### Desempenho contra baselines

* TSBS: o rank 1 V3 tem 0,548× a latência do esquema original do TSBS na coorte natural e 0,379× na alinhada.
* RUBiS: V3H01 = H01 tem 0,72–0,77× a latência de NoSE/QPG.
* IoT: TS2-U é o melhor medido (os baselines estão na V2).

### Custo de escrita e armazenamento

O vetor V3 penaliza mutações e armazenamento lógico na dominância. No TSBS, os deployments do HBench carregam de
88 mil a 111 mil mutações, contra 86,4 mil do TS0 sozinho, e ocupam 1,06–1,62 MB medidos. No TPCx, a projeção TS3
duplica as escritas (7 560 contra 3 780 por execução). O armazenamento medido acompanha o proxy lógico na ordem
dentro de cada benchmark.

### Reprodutibilidade entre sessões (RUBiS)

Os mesmos deployments medidos na V2 e na V3 (V3H01 = H01, V3H03 = H02, V3H04 = H04 e os 3 baselines) têm razão
V3/V2 entre 0,85 e 1,22. A ordem relativa H01 < NoSE/QPG se mantém. Os pisos A/A ficam entre 4,5% e 23% em 4.1, então
diferenças pequenas entre sessões não são interpretáveis.

### Conclusão em relação à hipótese da V3

Um Stage 7 único, com o mesmo vetor, a mesma dominância, a mesma viabilidade e a mesma diversidade nos 4
benchmarks, reduziu os espaços em 67–98% e manteve, em todos os casos, um deployment empatado com o melhor medido
dentro do denominador declarado, sem coeficientes de latência. As perdas foram duas: (i) a retenção da fronteira
medida de três objetivos é menor que a da V2 no TPCx, porque a V3 é mais seletiva; (ii) a diversidade estrutural pode
admitir deployments lentos (`V3H05`). Essas observações não alteram o HBench V3 congelado.


# Resultados detalhados (gerados)

## iot (medições V2 reutilizadas — D-V3-001)

Denominador: 3 V3 complete deployments, all measured in V2 verification mode. Piso de ruído A/A: 2.15%.

| seletor | HBench/espaço | DSR | Top-1 estrito | Top-1 (ruído) | Top-3 | near-best 5% | regret | retenção Pareto | τ (diag.) |
|---|---|---|---|---|---|---|---|---|---|
| V3 (Pareto + diversidade + balanced tiebreak) | 1/3 | 0.667 | True | True | True | True | 0.0000 | 1.000 | 1.00 |
| V2 (HBench antigo, mesmo denominador) | 3/3 | 0.000 | True | True | True | True | 0.0000 | 1.000 | 1.00 |

V2 conforme relatado na V2 (denominador da V2): {"space_size": 6, "hbench_size": 3, "top1_within_noise": true, "regret": 0.0, "pareto_retention": 1.0, "kendall_tau_score_vs_read": 0.8666666666666667}

| deployment V3 | V2 | HBench V3 | HBench V2 | média (ms) | IC 95% | armaz. (B) | escrita (s) | empata c/ melhor |
|---|---|---|---|---|---|---|---|---|
| `V3IOT_ts2_iot_wide_unbucketed` | R3H_38939db96444 | True | True | 4.145 | [4.089; 4.199] | 377683 | 6.83 | True |
| `V3IOT_ts0_iot_raw` | R3H_934e5af2474c | False | True | 5.455 | [5.389; 5.526] | 683543 | 15.19 | False |
| `V3IOT_ts2_iot_wide` | R3H_d2ea1974de53 | False | True | 6.924 | [6.853; 7.000] | 386433 | 7.92 | False |

### Ablações

| ablação | coorte/carga | |HBench| | status | Top-1 (ruído) | regret | retenção Pareto |
|---|---|---|---|---|---|---|
| full_v3 |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_read_rows |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_read_bytes |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_request_count |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_write_mutations |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_storage |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_max_partition_safety |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| with_risk_axes_in_dominance |  | 3 | EVALUATED | False | 0.0000 | 1.000 |
| without_structural_diversity |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| without_pareto |  | 3 | EVALUATED | True | 0.0000 | 1.000 |
| top_k_1 |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| random_k |  | 1 | EVALUATED | True | 0.0000 | 1.000 |

## tpcx_iot (medições V2 reutilizadas — D-V3-001)

Denominador: 9 V3 complete deployments, all measured in V2 verification mode. Piso de ruído A/A: 6.13%.

| seletor | HBench/espaço | DSR | Top-1 estrito | Top-1 (ruído) | Top-3 | near-best 5% | regret | retenção Pareto | τ (diag.) |
|---|---|---|---|---|---|---|---|---|---|
| V3 (Pareto + diversidade + balanced tiebreak) | 2/9 | 0.778 | False | False | True | True | 0.0017 | 0.333 | -0.34 |
| V2 (HBench antigo, mesmo denominador) | 5/9 | 0.444 | False | False | True | True | 0.0000 | 0.833 | -0.06 |

V2 conforme relatado na V2 (denominador da V2): {"space_size": 9, "hbench_size": 5, "top1_within_noise": false, "regret": 0.0, "pareto_retention": 0.8333333333333334, "kendall_tau_score_vs_read": -0.05555555555555555}

| deployment V3 | V2 | HBench V3 | HBench V2 | média (ms) | IC 95% | armaz. (B) | escrita (s) | empata c/ melhor |
|---|---|---|---|---|---|---|---|---|
| `h_a9983e2318a9` | h_a9983e2318a9 | False | True | 2.907 | [2.817; 2.998] | 351461 | 12.26 | True |
| `h_3f1449e05468` | h_3f1449e05468 | True | True | 2.912 | [2.837; 2.987] | 346561 | 12.48 | True |
| `h_6745757bf865` | h_6745757bf865 | False | True | 2.928 | [2.848; 3.007] | 346869 | 12.33 | True |
| `h_bdd6bbdce69f` | h_bdd6bbdce69f | False | False | 2.938 | [2.851; 3.038] | 348632 | 12.46 | True |
| `h_977291c72920` | h_977291c72920 | False | False | 2.950 | [2.869; 3.029] | 349461 | 12.35 | True |
| `h_6276d5b68197` | h_6276d5b68197 | False | False | 2.957 | [2.874; 3.037] | 349777 | 12.26 | True |
| `h_aba788b8e7b7` | h_aba788b8e7b7 | False | True | 4.569 | [4.428; 4.714] | 166807 | 7.90 | False |
| `h_119222012d4e` | h_119222012d4e | True | True | 4.671 | [4.533; 4.805] | 166445 | 7.95 | False |
| `h_b4bea9150d2e` | h_b4bea9150d2e | False | False | 5.061 | [4.902; 5.231] | 168587 | 7.96 | False |

### Ablações

| ablação | coorte/carga | |HBench| | status | Top-1 (ruído) | regret | retenção Pareto |
|---|---|---|---|---|---|---|
| full_v3 |  | 2 | EVALUATED | False | 0.0017 | 0.333 |
| without_read_rows |  | 2 | EVALUATED | False | 0.0017 | 0.333 |
| without_read_bytes |  | 2 | EVALUATED | False | 0.0017 | 0.333 |
| without_request_count |  | 1 | EVALUATED | False | 0.6070 | 0.167 |
| without_write_mutations |  | 2 | EVALUATED | False | 0.0017 | 0.333 |
| without_storage |  | 5 | EVALUATED | False | 0.0017 | 0.667 |
| without_max_partition_safety |  | 2 | EVALUATED | False | 0.0017 | 0.333 |
| with_risk_axes_in_dominance |  | 5 | EVALUATED | False | 0.0000 | 0.500 |
| without_structural_diversity |  | 2 | EVALUATED | False | 0.0017 | 0.333 |
| without_pareto |  | 5 | EVALUATED | False | 0.0107 | 0.500 |
| top_k_1 |  | 1 | EVALUATED | False | 0.6070 | 0.167 |
| random_k |  | 2 | EVALUATED | True | 0.0000 | 0.333 |

## tsbs (campanha V3 nova — verificação completa)

### coorte natural — piso A/A 12.29%; 24 V3 complete deployments (full verification)

| seletor | HBench/espaço | DSR | Top-1 estrito | Top-1 (ruído) | Top-3 | near-best 5% | regret | retenção Pareto | τ (diag.) |
|---|---|---|---|---|---|---|---|---|---|
| V3 | 5/24 | 0.792 | False | True | True | True | 0.0167 | 0.333 | 0.35 |

Rank 1 V3 / TSBS original: 0.548 [0.529; 0.568] → A_MAIS_RAPIDO; melhor do HBench (V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension) / TSBS original: 0.541 → A_MAIS_RAPIDO

### coorte aligned — piso A/A 4.94%; 24 V3 complete deployments (full verification)

| seletor | HBench/espaço | DSR | Top-1 estrito | Top-1 (ruído) | Top-3 | near-best 5% | regret | retenção Pareto | τ (diag.) |
|---|---|---|---|---|---|---|---|---|---|
| V3 | 5/24 | 0.792 | False | True | True | True | 0.0000 | 0.571 | 0.36 |

Rank 1 V3 / TSBS original: 0.379 [0.364; 0.394] → A_MAIS_RAPIDO; melhor do HBench (V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension) / TSBS original: 0.363 → A_MAIS_RAPIDO

| variante | deployment V3 | HBench V3 | natural (ms) | IC 95% | aligned (ms) | p95 nat. | armaz. (B) | carga (s) | escritas/s |
|---|---|---|---|---|---|---|---|---|---|
| H_476d7d568dda | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour` | False | 72.38 | [70.41; 74.40] | 72.70 | 318.78 | 1530340 | 32.4 | 3389 |
| H_475bc6375f77 | `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour` | False | 73.21 | [71.24; 75.27] | 73.57 | 307.75 | 1191576 | 27.9 | 3616 |
| H_86ec1f5bf644 | `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension` | True | 73.58 | [71.55; 75.61] | 49.01 | 336.35 | 1273895 | 28.4 | 3608 |
| H_182ac0f128a8 | `V3TSBS_ts0metricraw+ts5globalmaxminute` | False | 73.96 | [71.96; 75.95] | 73.02 | 334.77 | 981274 | 23.5 | 3692 |
| H_b232019d3428 | `V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension` | True | 74.42 | [72.32; 76.52] | 50.37 | 334.77 | 1061734 | 21.3 | 4135 |
| H_4829284e27af | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension` | True | 74.78 | [72.86; 76.68] | 49.03 | 332.26 | 1616959 | 34.7 | 3206 |
| H_0c3c75e05741 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5maxbyhosthour+ts6hourhostdimension` | False | 74.85 | [72.76; 76.91] | 51.77 | 346.20 | 1609058 | 33.0 | 3359 |
| H_36e89f0d48f7 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute` | False | 74.91 | [72.92; 76.93] | 73.41 | 334.27 | 1324787 | 28.9 | 3296 |
| H_a3de37f3ff0e | `V3TSBS_ts0metricraw+ts6hourhostdimension` | True | 75.08 | [72.97; 77.19] | 50.05 | 340.22 | 1057103 | 21.7 | 4040 |
| H_b9a08f03123b | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension` | True | 75.16 | [73.28; 77.07] | 48.20 | 342.75 | 1414609 | 26.6 | 3634 |
| H_16b788b7a733 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts6hourhostdimension` | False | 75.17 | [73.11; 77.22] | 49.23 | 345.21 | 1399932 | 28.0 | 3448 |
| H_4853a3699d99 | `V3TSBS_ts0metricraw+ts5maxbyhosthour` | False | 75.21 | [73.27; 77.24] | 76.00 | 336.81 | 1176036 | 25.4 | 3974 |
| H_4c07db3d50ba | `V3TSBS_ts0metricraw` | False | 75.25 | [73.19; 77.29] | 75.39 | 336.47 | 975570 | 25.1 | 3439 |
| H_1920a5c1aa7a | `V3TSBS_ts0metricraw+ts5maxbyhosthour+ts6hourhostdimension` | False | 75.64 | [73.69; 77.60] | 51.90 | 338.43 | 1266780 | 30.7 | 3326 |
| H_4fcba41a497a | `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour+ts5sumcountbyhosthour` | False | 75.66 | [73.77; 77.77] | 62.39 | 336.69 | 1431968 | 31.0 | 3722 |
| H_0a0a84ccc292 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5sumcountbyhosthour` | False | 75.77 | [73.76; 77.67] | 63.73 | 345.40 | 1566376 | 33.7 | 3252 |
| H_4bca45239282 | `V3TSBS_ts0metricraw+ts5sumcountbyhosthour` | False | 75.80 | [73.58; 77.98] | 65.39 | 340.16 | 1222024 | 27.3 | 3698 |
| H_9adc206d7a27 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5sumcountbyhosthour` | False | 78.47 | [76.45; 80.48] | 66.52 | 343.40 | 1564695 | 24.3 | 4514 |
| H_640a8a048068 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5maxbyhosthour` | False | 79.02 | [76.85; 81.17] | 78.04 | 354.87 | 1518203 | 28.3 | 3865 |
| H_c323ce593137 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5maxbyhosthour+ts5sumcountbyhosthour` | False | 79.41 | [77.44; 81.42] | 66.63 | 361.93 | 1756400 | 27.2 | 4550 |
| H_670b292f8a09 | `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts5sumcountbyhosthour` | False | 79.42 | [77.34; 81.50] | 64.53 | 355.91 | 1773004 | 30.5 | 4062 |
| H_d87bf4b15cf9 | `V3TSBS_ts0metricraw+ts5maxbyhosthour+ts5sumcountbyhosthour` | False | 79.69 | [77.67; 81.59] | 68.32 | 361.81 | 1417951 | 28.1 | 4105 |
| H_d0b3e35f8e02 | `V3TSBS_ts0metricraw+ts2widebucket1d` | False | 79.86 | [77.78; 81.97] | 78.92 | 357.67 | 1323831 | 22.7 | 4187 |
| H_b232019d3428_aa | `H_b232019d3428_aa` | False | 81.14 | [79.04; 83.30] | 51.18 | 368.79 | 1065054 | 25.1 | 3509 |
| H_a0e61f42ad5b | `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5sumcountbyhosthour` | False | 81.71 | [79.73; 83.78] | 66.27 | 369.51 | 1233504 | 28.2 | 3587 |
| B_tsbs_series_bigint | `B_tsbs_series_bigint` | False | 135.91 | [132.63; 139.06] | 132.89 | 718.23 | 1415565 | 25.5 | 3387 |

### Ablações

| ablação | coorte/carga | |HBench| | status | Top-1 (ruído) | regret | retenção Pareto |
|---|---|---|---|---|---|---|
| full_v3 | natural | 5 | EVALUATED | True | 0.0167 | 0.333 |
| without_read_rows | natural | 5 | EVALUATED | True | 0.0219 | 0.667 |
| without_read_bytes | natural | 5 | EVALUATED | True | 0.0219 | 0.667 |
| without_request_count | natural | 5 | EVALUATED | True | 0.0219 | 0.667 |
| without_write_mutations | natural | 5 | EVALUATED | True | 0.0167 | 0.333 |
| without_storage | natural | 5 | EVALUATED | True | 0.0167 | 0.333 |
| without_max_partition_safety | natural | 5 | EVALUATED | True | 0.0167 | 0.333 |
| with_risk_axes_in_dominance | natural | 5 | EVALUATED | True | 0.0219 | 0.667 |
| without_structural_diversity | natural | 5 | EVALUATED | True | 0.0167 | 0.500 |
| without_pareto | natural | 5 | EVALUATED | True | 0.0167 | 0.333 |
| top_k_1 | natural | 1 | EVALUATED | True | 0.0282 | 0.167 |
| random_k | natural | 5 | EVALUATED | True | 0.0341 | 0.000 |
| full_v3 | aligned | 5 | EVALUATED | True | 0.0000 | 0.571 |
| without_read_rows | aligned | 5 | EVALUATED | True | 0.0172 | 0.571 |
| without_read_bytes | aligned | 5 | EVALUATED | True | 0.0172 | 0.571 |
| without_request_count | aligned | 5 | EVALUATED | True | 0.0172 | 0.571 |
| without_write_mutations | aligned | 5 | EVALUATED | True | 0.0000 | 0.571 |
| without_storage | aligned | 5 | EVALUATED | True | 0.0000 | 0.571 |
| without_max_partition_safety | aligned | 5 | EVALUATED | True | 0.0000 | 0.571 |
| with_risk_axes_in_dominance | aligned | 5 | EVALUATED | True | 0.0172 | 0.571 |
| without_structural_diversity | aligned | 5 | EVALUATED | True | 0.0000 | 0.714 |
| without_pareto | aligned | 5 | EVALUATED | True | 0.0000 | 0.571 |
| top_k_1 | aligned | 1 | EVALUATED | True | 0.0451 | 0.143 |
| random_k | aligned | 5 | EVALUATED | False | 0.0741 | 0.000 |

## rubis (campanha V3 nova — 4.1.12 principal, 3.11.11 secundária)

Denominador: HBench V3 (5) + 3 baselines + A/A; the 256 complete deployments are not measured.

| braço/carga | melhor medido | empatados | ordem V3 | τ V3 | Top-1 V3 (ruído) | ordem NNLS legado (RETROSPECTIVE_BASELINE_ONLY) | τ NNLS | piso A/A % |
|---|---|---|---|---|---|---|---|---|
| v41/browsing | V3H01 | V3H01, V3H02, V3H03 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.80 | True | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.84 | 21.92 |
| v41/bidding | V3H01 | V3H01, V3H03 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.80 | True | V3H03 V3H01 V3H02 V3H04 V3H05 | 0.80 | 4.47 |
| v41/update10x | V3H01 | V3H01, V3H02, V3H03 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.80 | True | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.80 | 11.47 |
| v41/update100x | V3H04 | V3H01, V3H02, V3H03, V3H04, V3H05 | V3H01 V3H02 V3H03 V3H04 V3H05 | -0.60 | True | V3H05 V3H01 V3H02 V3H03 V3H04 | -0.20 | 23.00 |
| v311/browsing | V3H01 | V3H01, V3H02, V3H03 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.80 | True | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.84 | 9.24 |
| v311/bidding | V3H03 | V3H01, V3H03 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.60 | True | V3H03 V3H01 V3H02 V3H04 V3H05 | 1.00 | 6.52 |
| v311/update10x | V3H03 | V3H03 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.60 | False | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.60 | 5.74 |
| v311/update100x | V3H02 | V3H01, V3H02, V3H04 | V3H01 V3H02 V3H03 V3H04 V3H05 | 0.60 | True | V3H05 V3H01 V3H02 V3H03 V3H04 | -0.20 | 1.87 |

Hipóteses: **R1** sim — browsing: best=V3H01 vs NoSE=A_MAIS_RAPIDO; browsing: best=V3H01 vs QPG=A_MAIS_RAPIDO; bidding: best=V3H01 vs NoSE=A_MAIS_RAPIDO; bidding: best=V3H01 vs QPG=A_MAIS_RAPIDO; update10x: best=V3H01 vs NoSE=A_MAIS_RAPIDO; update10x: best=V3H01 vs QPG=A_MAIS_RAPIDO; **R2** sim — tau 3.11 x 4.1 (9 deployments): browsing=0.78, bidding=0.56, update10x=0.89, update100x=0.00; **ORACLE** sim — v311: 3577311/3577311; v41: 3577311/3577311

| braço | carga | deployment | média (ms) | IC 95% | p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|
| v41 | browsing | V3H01 | 1.237 | [1.222; 1.254] | 0.764 | 3.451 | 5.474 |
| v41 | browsing | NoSE | 1.699 | [1.682; 1.717] | 1.232 | 4.389 | 6.810 |
| v41 | browsing | V3H02 | 1.303 | [1.287; 1.321] | 0.762 | 4.048 | 6.019 |
| v41 | browsing | V8_AUTO_H_norm | 2.066 | [2.047; 2.088] | 1.442 | 5.325 | 9.446 |
| v41 | browsing | V3H03 | 1.294 | [1.279; 1.310] | 0.786 | 3.869 | 5.857 |
| v41 | browsing | QPG | 1.609 | [1.592; 1.626] | 1.302 | 3.764 | 6.300 |
| v41 | browsing | V3H04 | 1.793 | [1.771; 1.814] | 1.055 | 5.606 | 12.109 |
| v41 | browsing | V3H05 | 4.665 | [4.624; 4.705] | 3.546 | 12.966 | 13.515 |
| v41 | browsing | V3H01_AA | 1.029 | [1.017; 1.043] | 0.641 | 2.479 | 4.409 |
| v41 | bidding | V3H01 | 1.570 | [1.550; 1.592] | 1.069 | 4.420 | 6.919 |
| v41 | bidding | NoSE | 2.175 | [2.144; 2.210] | 1.422 | 6.118 | 11.500 |
| v41 | bidding | V3H02 | 1.655 | [1.637; 1.677] | 1.117 | 4.955 | 7.217 |
| v41 | bidding | V8_AUTO_H_norm | 2.400 | [2.370; 2.434] | 1.543 | 6.688 | 12.153 |
| v41 | bidding | V3H03 | 1.599 | [1.579; 1.622] | 1.097 | 4.730 | 7.150 |
| v41 | bidding | QPG | 2.058 | [2.028; 2.088] | 1.358 | 5.861 | 9.502 |
| v41 | bidding | V3H04 | 2.096 | [2.070; 2.122] | 1.310 | 6.567 | 12.779 |
| v41 | bidding | V3H05 | 3.762 | [3.726; 3.801] | 2.354 | 12.734 | 13.223 |
| v41 | bidding | V3H01_AA | 1.525 | [1.503; 1.549] | 1.017 | 4.476 | 7.081 |
| v41 | update10x | V3H01 | 1.725 | [1.702; 1.752] | 1.395 | 4.373 | 7.250 |
| v41 | update10x | NoSE | 2.419 | [2.372; 2.468] | 1.548 | 5.759 | 17.328 |
| v41 | update10x | V3H02 | 1.777 | [1.752; 1.805] | 1.410 | 4.498 | 7.305 |
| v41 | update10x | V8_AUTO_H_norm | 2.452 | [2.411; 2.496] | 1.439 | 6.846 | 18.716 |
| v41 | update10x | V3H03 | 1.759 | [1.736; 1.784] | 1.429 | 4.275 | 7.062 |
| v41 | update10x | QPG | 2.280 | [2.231; 2.337] | 1.396 | 6.264 | 17.238 |
| v41 | update10x | V3H04 | 1.946 | [1.919; 1.974] | 1.429 | 5.502 | 9.113 |
| v41 | update10x | V3H05 | 2.820 | [2.788; 2.855] | 1.816 | 9.017 | 13.020 |
| v41 | update10x | V3H01_AA | 1.574 | [1.551; 1.602] | 1.370 | 3.807 | 6.467 |
| v41 | update100x | V3H01 | 2.007 | [1.989; 2.027] | 1.578 | 4.136 | 7.036 |
| v41 | update100x | NoSE | 2.066 | [2.036; 2.100] | 1.550 | 4.288 | 7.803 |
| v41 | update100x | V3H02 | 1.839 | [1.817; 1.864] | 1.427 | 4.070 | 6.521 |
| v41 | update100x | V8_AUTO_H_norm | 2.158 | [2.125; 2.193] | 1.428 | 3.994 | 9.353 |
| v41 | update100x | V3H03 | 1.880 | [1.861; 1.903] | 1.462 | 4.043 | 6.023 |
| v41 | update100x | QPG | 1.929 | [1.900; 1.959] | 1.418 | 3.848 | 8.997 |
| v41 | update100x | V3H04 | 1.763 | [1.742; 1.788] | 1.403 | 3.999 | 6.118 |
| v41 | update100x | V3H05 | 1.816 | [1.793; 1.842] | 1.247 | 3.400 | 8.450 |
| v41 | update100x | V3H01_AA | 1.649 | [1.634; 1.668] | 1.392 | 3.882 | 5.063 |
| v311 | browsing | V3H01 | 0.866 | [0.854; 0.881] | 0.594 | 2.203 | 3.663 |
| v311 | browsing | NoSE | 1.104 | [1.090; 1.120] | 0.965 | 2.691 | 3.604 |
| v311 | browsing | V3H02 | 0.901 | [0.889; 0.916] | 0.614 | 2.216 | 3.807 |
| v311 | browsing | V8_AUTO_H_norm | 1.409 | [1.393; 1.426] | 1.029 | 3.581 | 5.802 |
| v311 | browsing | V3H03 | 0.894 | [0.883; 0.907] | 0.601 | 2.201 | 3.909 |
| v311 | browsing | QPG | 1.182 | [1.166; 1.200] | 0.981 | 2.781 | 4.162 |
| v311 | browsing | V3H04 | 1.365 | [1.346; 1.384] | 0.735 | 4.501 | 8.107 |
| v311 | browsing | V3H05 | 3.220 | [3.190; 3.251] | 2.014 | 10.610 | 11.165 |
| v311 | browsing | V3H01_AA | 0.932 | [0.919; 0.945] | 0.623 | 2.335 | 3.961 |
| v311 | bidding | V3H01 | 1.139 | [1.121; 1.159] | 0.764 | 2.668 | 4.477 |
| v311 | bidding | NoSE | 1.624 | [1.596; 1.655] | 1.082 | 4.006 | 9.159 |
| v311 | bidding | V3H02 | 1.196 | [1.179; 1.215] | 0.784 | 3.139 | 5.033 |
| v311 | bidding | V8_AUTO_H_norm | 1.594 | [1.568; 1.621] | 1.145 | 4.396 | 6.767 |
| v311 | bidding | V3H03 | 1.084 | [1.069; 1.104] | 0.744 | 2.428 | 3.924 |
| v311 | bidding | QPG | 1.627 | [1.599; 1.657] | 1.089 | 4.603 | 7.639 |
| v311 | bidding | V3H04 | 1.607 | [1.585; 1.632] | 0.984 | 4.940 | 8.674 |
| v311 | bidding | V3H05 | 2.868 | [2.838; 2.898] | 1.747 | 10.222 | 10.990 |
| v311 | bidding | V3H01_AA | 1.191 | [1.171; 1.212] | 0.798 | 2.921 | 4.698 |
| v311 | update10x | V3H01 | 1.415 | [1.393; 1.440] | 1.299 | 3.109 | 4.729 |
| v311 | update10x | NoSE | 1.834 | [1.800; 1.874] | 1.358 | 3.883 | 13.251 |
| v311 | update10x | V3H02 | 1.420 | [1.397; 1.446] | 1.271 | 3.407 | 5.453 |
| v311 | update10x | V8_AUTO_H_norm | 1.853 | [1.816; 1.897] | 1.255 | 4.581 | 12.268 |
| v311 | update10x | V3H03 | 1.326 | [1.306; 1.350] | 1.254 | 2.896 | 4.571 |
| v311 | update10x | QPG | 1.718 | [1.684; 1.756] | 1.230 | 4.117 | 12.280 |
| v311 | update10x | V3H04 | 1.586 | [1.563; 1.613] | 1.330 | 4.234 | 7.000 |
| v311 | update10x | V3H05 | 2.521 | [2.491; 2.554] | 1.702 | 7.535 | 10.849 |
| v311 | update10x | V3H01_AA | 1.366 | [1.342; 1.393] | 1.281 | 2.875 | 4.280 |
| v311 | update100x | V3H01 | 1.647 | [1.626; 1.672] | 1.386 | 3.572 | 4.894 |
| v311 | update100x | NoSE | 1.722 | [1.694; 1.755] | 1.407 | 3.586 | 6.223 |
| v311 | update100x | V3H02 | 1.645 | [1.625; 1.670] | 1.409 | 3.549 | 4.716 |
| v311 | update100x | V8_AUTO_H_norm | 1.693 | [1.671; 1.718] | 1.279 | 3.218 | 7.148 |
| v311 | update100x | V3H03 | 1.737 | [1.717; 1.763] | 1.408 | 3.610 | 5.035 |
| v311 | update100x | QPG | 1.528 | [1.503; 1.556] | 1.258 | 3.188 | 6.734 |
| v311 | update100x | V3H04 | 1.650 | [1.628; 1.675] | 1.382 | 3.576 | 5.637 |
| v311 | update100x | V3H05 | 1.757 | [1.735; 1.783] | 1.247 | 3.132 | 8.238 |
| v311 | update100x | V3H01_AA | 1.649 | [1.630; 1.672] | 1.391 | 3.583 | 4.698 |

### Reprodutibilidade entre sessões (mesmo deployment medido na V2 e na V3)

| braço | carga | V3 | V2 | V3 (ms) | V2 (ms) | razão |
|---|---|---|---|---|---|---|
| v41 | browsing | V3H01 | H01 | 1.237 | 1.205 | 1.027 |
| v41 | browsing | V3H03 | H02 | 1.294 | 1.060 | 1.221 |
| v41 | browsing | V3H04 | H04 | 1.793 | 1.659 | 1.080 |
| v41 | bidding | V3H01 | H01 | 1.570 | 1.474 | 1.065 |
| v41 | bidding | V3H03 | H02 | 1.599 | 1.464 | 1.093 |
| v41 | bidding | V3H04 | H04 | 2.096 | 2.222 | 0.943 |
| v41 | update10x | V3H01 | H01 | 1.725 | 1.650 | 1.046 |
| v41 | update10x | V3H03 | H02 | 1.759 | 1.457 | 1.208 |
| v41 | update10x | V3H04 | H04 | 1.946 | 2.148 | 0.906 |
| v41 | update100x | V3H01 | H01 | 2.007 | 1.702 | 1.179 |
| v41 | update100x | V3H03 | H02 | 1.880 | 1.792 | 1.049 |
| v41 | update100x | V3H04 | H04 | 1.763 | 2.068 | 0.853 |
| v311 | browsing | V3H01 | H01 | 0.866 | 0.877 | 0.988 |
| v311 | browsing | V3H03 | H02 | 0.894 | 0.830 | 1.077 |
| v311 | browsing | V3H04 | H04 | 1.365 | 1.282 | 1.065 |
| v311 | bidding | V3H01 | H01 | 1.139 | 1.156 | 0.985 |
| v311 | bidding | V3H03 | H02 | 1.084 | 1.131 | 0.959 |
| v311 | bidding | V3H04 | H04 | 1.607 | 1.580 | 1.017 |
| v311 | update10x | V3H01 | H01 | 1.415 | 1.358 | 1.042 |
| v311 | update10x | V3H03 | H02 | 1.326 | 1.477 | 0.898 |
| v311 | update10x | V3H04 | H04 | 1.586 | 1.613 | 0.984 |
| v311 | update100x | V3H01 | H01 | 1.647 | 1.715 | 0.960 |
| v311 | update100x | V3H03 | H02 | 1.737 | 1.729 | 1.004 |
| v311 | update100x | V3H04 | H04 | 1.650 | 1.621 | 1.018 |
| v41 | browsing | NoSE | NoSE | 1.699 | 1.614 | 1.053 |
| v41 | browsing | QPG | QPG | 1.609 | 1.873 | 0.859 |
| v41 | browsing | V8_AUTO_H_norm | V8_AUTO_H_norm | 2.066 | 1.971 | 1.049 |
| v41 | bidding | NoSE | NoSE | 2.175 | 2.226 | 0.977 |
| v41 | bidding | QPG | QPG | 2.058 | 2.214 | 0.929 |
| v41 | bidding | V8_AUTO_H_norm | V8_AUTO_H_norm | 2.400 | 2.534 | 0.947 |
| v41 | update10x | NoSE | NoSE | 2.419 | 2.295 | 1.054 |
| v41 | update10x | QPG | QPG | 2.280 | 2.240 | 1.018 |
| v41 | update10x | V8_AUTO_H_norm | V8_AUTO_H_norm | 2.452 | 2.444 | 1.003 |
| v41 | update100x | NoSE | NoSE | 2.066 | 1.799 | 1.149 |
| v41 | update100x | QPG | QPG | 1.929 | 1.645 | 1.172 |
| v41 | update100x | V8_AUTO_H_norm | V8_AUTO_H_norm | 2.158 | 2.375 | 0.908 |
| v311 | browsing | NoSE | NoSE | 1.104 | 1.140 | 0.969 |
| v311 | browsing | QPG | QPG | 1.182 | 1.239 | 0.954 |
| v311 | browsing | V8_AUTO_H_norm | V8_AUTO_H_norm | 1.409 | 1.269 | 1.110 |
| v311 | bidding | NoSE | NoSE | 1.624 | 1.608 | 1.010 |
| v311 | bidding | QPG | QPG | 1.627 | 1.418 | 1.148 |
| v311 | bidding | V8_AUTO_H_norm | V8_AUTO_H_norm | 1.594 | 1.587 | 1.004 |
| v311 | update10x | NoSE | NoSE | 1.834 | 1.776 | 1.033 |
| v311 | update10x | QPG | QPG | 1.718 | 1.551 | 1.107 |
| v311 | update10x | V8_AUTO_H_norm | V8_AUTO_H_norm | 1.853 | 1.839 | 1.007 |
| v311 | update100x | NoSE | NoSE | 1.722 | 1.783 | 0.966 |
| v311 | update100x | QPG | QPG | 1.528 | 1.481 | 1.032 |
| v311 | update100x | V8_AUTO_H_norm | V8_AUTO_H_norm | 1.693 | 1.730 | 0.979 |

### Ablações (RUBiS: só avaliáveis quando o HBench ablado foi medido)

| ablação | coorte/carga | |HBench| | status | Top-1 (ruído) | regret | retenção Pareto |
|---|---|---|---|---|---|---|
| full_v3 |  | 5 | EVALUATED | True | 0.0000 | 1.000 |
| without_read_rows |  | 5 | EVALUATED | True | 0.0000 | 1.000 |
| without_read_bytes |  | 5 | NOT_EVALUABLE (2 unmeasured) | — | — | — |
| without_request_count |  | 5 | NOT_EVALUABLE (2 unmeasured) | — | — | — |
| without_write_mutations |  | 5 | EVALUATED | True | 0.0000 | 1.000 |
| without_storage |  | 5 | NOT_EVALUABLE (1 unmeasured) | — | — | — |
| without_max_partition_safety |  | 5 | EVALUATED | True | 0.0000 | 1.000 |
| with_risk_axes_in_dominance |  | 5 | EVALUATED | True | 0.0000 | 1.000 |
| without_structural_diversity |  | 5 | NOT_EVALUABLE (2 unmeasured) | — | — | — |
| without_pareto |  | 5 | NOT_EVALUABLE (3 unmeasured) | — | — | — |
| top_k_1 |  | 1 | EVALUATED | True | 0.0000 | 1.000 |
| random_k |  | 5 | NOT_EVALUABLE (5 unmeasured) | — | — | — |

