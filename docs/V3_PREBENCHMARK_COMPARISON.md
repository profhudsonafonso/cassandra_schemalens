# V3 — Comparação pré-benchmark V2 × V3

Gerado **antes** de qualquer execução Cassandra da V3 (`python -m unified_stage7_v3.report_prebenchmark`). Lê apenas artefatos de projeto (pré-registros V2, H.json da V9) e as saídas do Stage 7/8 V3; nenhum resultado medido. **Não se interpreta aqui qual HBench é melhor.** Identidade de deployment = conjunto de tabelas; a igualdade de rotas é reportada à parte porque decide a reutilização das medições (D-V3-001).

## iot

- **Universo de candidatos:** 4 CandidateSpecs; 6 deployments enumerados, 3 completos/viáveis (V2: espaço admissível = 6).
- **Cenário (escala da campanha):** `{"benchmark": "IoT-Benchmark", "campaign": "B Rev3 (DATA_SEED 531101, QUERY_SEED 531107)", "devices": 9, "executed_stage9_scenario": "identical (same CSV and query objects measured in V2)", "history_days": 20.826, "metric_cells": 54000}`
- **Frequências:** EXPERIMENTAL_POLICY_UNIFORM / UNIFORM_QUERY_CLASS
- **Método analítico antigo:** Rev3 plan-aware (V2): proxies de requisições/linhas/bytes por plano + escore de pré-seleção ponderado (0,8 leitura / 0,1 armazenamento / 0,1 escrita), Pareto 3D (leitura, armazenamento, escrita), top-k
- **Método novo:** PhysicalWorkVector (read_cql_requests, read_logical_rows, read_logical_bytes, write_cql_mutations, maintenance_read_requests, logical_storage_bytes) → cobertura/viabilidade → Pareto (6 eixos) → diversidade estrutural → balanced_structural_tiebreak
- **Fronteira de Pareto V3:** 1 deployments; regra aplicada: `PARETO_FITS_TOP_K_KEEP_ALL`; |HBench V3| = 1
- **HBench antigo (V2):** 3 — `R3H_38939db96444`, `R3H_934e5af2474c`, `R3H_d2ea1974de53`
- **HBench novo (V3):** 1 — `V3IOT_ts2_iot_wide_unbucketed`
- **Interseção (mesmas tabelas):** 1; **só antigo:** 2; **só novo:** 0; **Jaccard:** 0.333
- **Rotas idênticas às da V2 na interseção:** R3H_38939db96444=sim

| deployment V3 | tabelas | V2 | razão (retido/excluído) | req. | linhas | bytes | mutações | leit. manut. | armaz. (B) |
|---|---|---|---|---|---|---|---|---|---|
| `V3IOT_ts2_iot_wide_unbucketed` | ts2_iot_wide_unbucketed | R3H_38939db96444 | HBENCH: PARETO_FITS_TOP_K_KEEP_ALL | 1.083 | 1090.5 | 26172 | 27000.000 | 0.000 | 729000 |
| `V3IOT_ts0_iot_raw` | ts0_iot_raw | R3H_934e5af2474c | DOMINADO por V3IOT_ts2_iot_wide_unbucketed | 2.167 | 2181.0 | 34896 | 54000.000 | 0.000 | 1188000 |
| `V3IOT_ts2_iot_wide` | ts2_iot_wide | R3H_d2ea1974de53 | DOMINADO por V3IOT_ts2_iot_wide_unbucketed | 8.583 | 1090.5 | 26172 | 27000.000 | 0.000 | 837000 |

Inviáveis: 3 — motivos (contagem de ocorrências): dead table: 3
- `R3H_934e5af2474c` (só V2): DOMINADO por V3IOT_ts2_iot_wide_unbucketed
- `R3H_d2ea1974de53` (só V2): DOMINADO por V3IOT_ts2_iot_wide_unbucketed

## tpcx_iot

- **Universo de candidatos:** 5 CandidateSpecs; 9 deployments enumerados, 9 completos/viáveis (V2: espaço admissível = 9).
- **Cenário (escala da campanha):** `{"bench_steps": 500, "benchmark": "TPCx-IoT (research adaptation)", "events_per_execution": 3780, "executed_stage9_scenario": "identical to the V2 campaign (tpcx_v1 WORKLOAD)", "mean_rows_per_logical_read": 22.0, "old_stage7_scenario_NOT_USED": "10 stations x 200 sensors, 60 s, 1 day, RF=2"}`
- **Frequências:** FROZEN_WORKLOAD_PARAMETERS (120 reads and 3780 ingested events per execution) / PER_EXECUTION
- **Método analítico antigo:** Stage 7/8 TPCx (V2): cenário ilustrativo 10 estações × 200 sensores, RF 2; escore normalizado de proxies; Pareto 3D; top_k 5
- **Método novo:** PhysicalWorkVector (read_cql_requests, read_logical_rows, read_logical_bytes, write_cql_mutations, maintenance_read_requests, logical_storage_bytes) → cobertura/viabilidade → Pareto (6 eixos) → diversidade estrutural → balanced_structural_tiebreak
- **Fronteira de Pareto V3:** 2 deployments; regra aplicada: `PARETO_FITS_TOP_K_KEEP_ALL`; |HBench V3| = 2
- **HBench antigo (V2):** 5 — `h_119222012d4e`, `h_6745757bf865`, `h_a9983e2318a9`, `h_3f1449e05468`, `h_aba788b8e7b7`
- **HBench novo (V3):** 2 — `h_119222012d4e`, `h_3f1449e05468`
- **Interseção (mesmas tabelas):** 2; **só antigo:** 3; **só novo:** 0; **Jaccard:** 0.400
- **Rotas idênticas às da V2 na interseção:** h_119222012d4e=sim, h_3f1449e05468=sim

| deployment V3 | tabelas | V2 | razão (retido/excluído) | req. | linhas | bytes | mutações | leit. manut. | armaz. (B) |
|---|---|---|---|---|---|---|---|---|---|
| `h_119222012d4e` | ts0_series_raw | h_119222012d4e | HBENCH: PARETO_FITS_TOP_K_KEEP_ALL | 4.000 | 22.0 | 20834 | 3780.000 | 0.000 | 3579660 |
| `h_3f1449e05468` | ts0_series_raw + ts3_sensor_3600s | h_3f1449e05468 | HBENCH: PARETO_FITS_TOP_K_KEEP_ALL | 2.000 | 22.0 | 20834 | 7560.000 | 0.000 | 7189560 |
| `h_aba788b8e7b7` | ts1_series_3600s | h_aba788b8e7b7 | DOMINADO por h_119222012d4e | 4.000 | 22.0 | 20834 | 3780.000 | 0.000 | 3609900 |
| `h_b4bea9150d2e` | ts1_series_60s |  | DOMINADO por h_119222012d4e | 4.300 | 22.0 | 20834 | 3780.000 | 0.000 | 3609900 |
| `h_6745757bf865` | ts1_series_3600s + ts3_sensor_3600s | h_6745757bf865 | DOMINADO por h_3f1449e05468 | 2.000 | 22.0 | 20834 | 7560.000 | 0.000 | 7219800 |
| `h_bdd6bbdce69f` | ts1_series_60s + ts3_sensor_3600s |  | DOMINADO por h_3f1449e05468 | 2.000 | 22.0 | 20834 | 7560.000 | 0.000 | 7219800 |
| `h_977291c72920` | ts0_series_raw + ts3_sensor_60s |  | DOMINADO por h_3f1449e05468 | 2.150 | 22.0 | 20834 | 7560.000 | 0.000 | 7189560 |
| `h_6276d5b68197` | ts1_series_3600s + ts3_sensor_60s |  | DOMINADO por h_3f1449e05468 | 2.150 | 22.0 | 20834 | 7560.000 | 0.000 | 7219800 |
| `h_a9983e2318a9` | ts1_series_60s + ts3_sensor_60s | h_a9983e2318a9 | DOMINADO por h_3f1449e05468 | 2.150 | 22.0 | 20834 | 7560.000 | 0.000 | 7219800 |
- `h_aba788b8e7b7` (só V2): DOMINADO por h_119222012d4e
- `h_6745757bf865` (só V2): DOMINADO por h_3f1449e05468
- `h_a9983e2318a9` (só V2): DOMINADO por h_3f1449e05468

## tsbs

- **Universo de candidatos:** 9 CandidateSpecs; 256 deployments enumerados, 24 completos/viáveis (V2: espaço admissível = 26).
- **Cenário (escala da campanha):** `{"benchmark": "TSBS cpu-only", "cohorts": ["natural", "aligned"], "data_sha256": "47efcf29f8175ee4b5af5f910f49ed03200029fb4c00ac0bf44d58728c6eed7c", "executed_stage9_scenario": "identical to the V2 campaign (same data file and instance file)", "generator": "tsbs_generate_data commit 8323e59c, --scale=40 --log-interval=600s, 36 h, seed 20261117", "hosts": 40}`
- **Frequências:** UNSPECIFIED / UNIFORM_QUERY_CLASS (6 frozen instances per template: 3 natural + 3 aligned)
- **Método analítico antigo:** Stage 7/8 TSBS (V2): cenário 100 hosts / 60 s / 3 dias; escore ponderado 0,8 leitura / 0,1 armazenamento / 0,1 chaves de escrita; perfis conservative e conditional_aligned; Pareto 3D; top_k 5 por perfil
- **Método novo:** PhysicalWorkVector (read_cql_requests, read_logical_rows, read_logical_bytes, write_cql_mutations, maintenance_read_requests, logical_storage_bytes) → cobertura/viabilidade → Pareto (6 eixos) → diversidade estrutural → balanced_structural_tiebreak
- **Fronteira de Pareto V3:** 7 deployments; regra aplicada: `PARETO_THEN_STRUCTURAL_DIVERSITY_THEN_TIEBREAK`; |HBench V3| = 5
- **HBench antigo (V2):** 7 — `H_815d3cd1f0cb`, `H_df2016403111`, `H_077824c77c40`, `H_ea367cb023b3`, `H_69cc46e6ed43`, `H_e08177e1a2a8`, `H_b6f44ac379f4`
- **HBench novo (V3):** 5 — `V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension`, `V3TSBS_ts0metricraw+ts6hourhostdimension`, `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension`, `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension`, `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension`
- **Interseção (mesmas tabelas):** 5; **só antigo:** 2; **só novo:** 0; **Jaccard:** 0.714
- **Rotas idênticas às da V2 na interseção:** H_ea367cb023b3=não, H_077824c77c40=sim, H_e08177e1a2a8=não, H_69cc46e6ed43=sim, H_b6f44ac379f4=sim

| deployment V3 | tabelas | V2 | razão (retido/excluído) | req. | linhas | bytes | mutações | leit. manut. | armaz. (B) |
|---|---|---|---|---|---|---|---|---|---|
| `V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension` | ts0_metric_raw + ts5_global_max_minute + ts6_hour_host_dimension | H_69cc46e6ed43 | HBENCH: lowest balanced_structural_tiebreak | 102.417 | 6716.6 | 114250 | 88056.000 | 0.000 | 3214742 |
| `V3TSBS_ts0metricraw+ts6hourhostdimension` | ts0_metric_raw + ts6_hour_host_dimension | H_b6f44ac379f4 | HBENCH: structural diversity (novas tags: family:ts0_metric_raw+ts6_hour_host_dimension) | 103.625 | 6843.5 | 116280 | 87840.000 | 0.000 | 3207960 |
| `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension` | ts0_metric_raw + ts5_global_max_minute + ts5_max_by_host_hour + ts6_hour_host_dimension | H_e08177e1a2a8 | HBENCH: structural diversity (novas tags: family:ts0_metric_raw+ts5_global_max_minute+ts5_max_by_host_hour+ts6_hour_host_dimension) | 102.417 | 6641.6 | 113050 | 102456.000 | 0.000 | 3764102 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_global_max_minute + ts6_hour_host_dimension | H_077824c77c40 | HBENCH: structural diversity (novas tags: family:ts0_metric_raw+ts2_wide_bucket_1d+ts5_global_max_minute+ts6_hour_host_dimension) | 101.885 | 6676.1 | 113926 | 96696.000 | 0.000 | 4067942 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_global_max_minute + ts5_max_by_host_hour + ts6_hour_host_dimension | H_ea367cb023b3 | HBENCH: structural diversity (novas tags: bucket:max_by_host_hour, bucket:wide_bucket_1d, family:ts0_metric_raw+ts2_wide_bucket_1d+ts5_global_max_minute+ts5_max_by_host_hour+ts6_hour_host_dimension, table:ts2_wide_bucket_1d) | 101.885 | 6601.1 | 112726 | 111096.000 | 0.000 | 4617302 |
| `V3TSBS_ts0metricraw+ts5globalmaxminute` | ts0_metric_raw + ts5_global_max_minute |  | PARETO, cortado pelo top_k após diversidade/desempate | 121.292 | 8111.6 | 129786 | 86616.000 | 0.000 | 2957342 |
| `V3TSBS_ts0metricraw` | ts0_metric_raw | H_df2016403111 | PARETO, cortado pelo top_k após diversidade/desempate | 122.500 | 8238.5 | 131816 | 86400.000 | 0.000 | 2950560 |
| `V3TSBS_ts0metricraw+ts5maxbyhosthour+ts6hourhostdimension` | ts0_metric_raw + ts5_max_by_host_hour + ts6_hour_host_dimension |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension | 103.625 | 6768.5 | 115080 | 102240.000 | 0.000 | 3757320 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts6hourhostdimension` | ts0_metric_raw + ts2_wide_bucket_1d + ts6_hour_host_dimension |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension | 103.094 | 6803.0 | 115956 | 96480.000 | 0.000 | 4061160 |
| `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5sumcountbyhosthour` | ts0_metric_raw + ts5_global_max_minute + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension | 121.292 | 7286.6 | 117906 | 101016.000 | 0.000 | 3621902 |
| `V3TSBS_ts0metricraw+ts5sumcountbyhosthour` | ts0_metric_raw + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension | 122.500 | 7413.5 | 119936 | 100800.000 | 0.000 | 3615120 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5maxbyhosthour+ts6hourhostdimension` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_max_by_host_hour + ts6_hour_host_dimension |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 103.094 | 6728.0 | 114756 | 110880.000 | 0.000 | 4610520 |
| `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour` | ts0_metric_raw + ts5_global_max_minute + ts5_max_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension | 121.292 | 8036.6 | 128586 | 101016.000 | 0.000 | 3506702 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_global_max_minute |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute+ts6hourhostdimension | 120.760 | 8071.1 | 129462 | 95256.000 | 0.000 | 3810542 |
| `V3TSBS_ts0metricraw+ts5maxbyhosthour` | ts0_metric_raw + ts5_max_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute | 122.500 | 8163.5 | 130616 | 100800.000 | 0.000 | 3499920 |
| `V3TSBS_ts0metricraw+ts2widebucket1d` | ts0_metric_raw + ts2_wide_bucket_1d | H_815d3cd1f0cb | DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute | 121.969 | 8198.0 | 131492 | 95040.000 | 0.000 | 3803760 |
| `V3TSBS_ts0metricraw+ts5globalmaxminute+ts5maxbyhosthour+ts5sumcountbyhosthour` | ts0_metric_raw + ts5_global_max_minute + ts5_max_by_host_hour + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 121.292 | 7211.6 | 116706 | 115416.000 | 0.000 | 4171262 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5sumcountbyhosthour` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_global_max_minute + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 120.760 | 7246.1 | 117582 | 109656.000 | 0.000 | 4475102 |
| `V3TSBS_ts0metricraw+ts5maxbyhosthour+ts5sumcountbyhosthour` | ts0_metric_raw + ts5_max_by_host_hour + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 122.500 | 7338.5 | 118736 | 115200.000 | 0.000 | 4164480 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5sumcountbyhosthour` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 121.969 | 7373.0 | 119612 | 109440.000 | 0.000 | 4468320 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_global_max_minute + ts5_max_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 120.760 | 7996.1 | 128262 | 109656.000 | 0.000 | 4359902 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5maxbyhosthour` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_max_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts6hourhostdimension | 121.969 | 8123.0 | 130292 | 109440.000 | 0.000 | 4353120 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts5sumcountbyhosthour` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_global_max_minute + ts5_max_by_host_hour + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension | 120.760 | 7171.1 | 116382 | 124056.000 | 0.000 | 5024462 |
| `V3TSBS_ts0metricraw+ts2widebucket1d+ts5maxbyhosthour+ts5sumcountbyhosthour` | ts0_metric_raw + ts2_wide_bucket_1d + ts5_max_by_host_hour + ts5_sum_count_by_host_hour |  | DOMINADO por V3TSBS_ts0metricraw+ts2widebucket1d+ts5globalmaxminute+ts5maxbyhosthour+ts6hourhostdimension | 121.969 | 7298.0 | 118412 | 123840.000 | 0.000 | 5017680 |

Inviáveis: 232 — motivos (contagem de ocorrências): dead table: 448
- `H_df2016403111` (só V2): PARETO, cortado pelo top_k após diversidade/desempate
- `H_815d3cd1f0cb` (só V2): DOMINADO por V3TSBS_ts0metricraw+ts5globalmaxminute

## rubis

- **Universo de candidatos:** 24 CandidateSpecs; 32768 deployments enumerados, 256 completos/viáveis (V2: espaço admissível = n/d (ILP)).
- **Cenário (escala da campanha):** `{"benchmark": "RUBiS (SchemaLens V9 design space)", "cohort_sha256": "624633a5068ad5076544127524cf779d116860efc4a51b25e0bde6c6472904b3", "entity_counts": {"bids": 5690, "buynow": 5300, "categories": 500, "comments": 6529, "items": 7392, "regions": 50, "users": 21855}, "frequency_aggregation": "MEAN_OF_E05_MIXES", "frequency_source": "E05", "mandatory_tables": ["C001", "C002", "C003", "C004", "C014`
- **Frequências:** E05 statement_frequency_per_request / MEAN_OF_E05_MIXES (browsing, bidding, update10x, update100x; equal weight, D-V3-009)
- **Método analítico antigo:** V9 E14/E15: modelo de custo NNLS calibrado (a_read 0,462 ms, b_write 0,389 ms, c_row 0,0075 ms) + ILP por mix, grade de somas ponderadas (Pareto leitura × escrita × espaço, mix bidding) e k-melhores até |H| = 5
- **Método novo:** PhysicalWorkVector (read_cql_requests, read_logical_rows, read_logical_bytes, write_cql_mutations, maintenance_read_requests, logical_storage_bytes) → cobertura/viabilidade → Pareto (6 eixos) → diversidade estrutural → balanced_structural_tiebreak
- **Fronteira de Pareto V3:** 20 deployments; regra aplicada: `PARETO_THEN_STRUCTURAL_DIVERSITY_THEN_TIEBREAK`; |HBench V3| = 5
- **HBench antigo (V2):** 5 — `H01`, `H02`, `H03`, `H04`, `H05`
- **HBench novo (V3):** 5 — `V3R_cb2e537945`, `V3R_a82100f407`, `V3R_c3fc027f9f`, `V3R_0fbc71f963`, `V3R_63f05eeb86`
- **Interseção (mesmas tabelas):** 3; **só antigo:** 2; **só novo:** 2; **Jaccard:** 0.429
- **Rotas idênticas às da V2 na interseção:** H02=sim, H01=sim, H04=sim

| deployment V3 | tabelas | V2 | razão (retido/excluído) | req. | linhas | bytes | mutações | leit. manut. | armaz. (B) |
|---|---|---|---|---|---|---|---|---|---|
| `V3R_cb2e537945` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C016 + C017 + C018 + C020 + C021 + C024 | H01 | HBENCH: lowest balanced_structural_tiebreak | 1.008 | 31.8 | 1017 | 1.276 | 0.719 | 7406729 |
| `V3R_a82100f407` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | HBENCH: tiebreak | 1.008 | 31.8 | 1014 | 1.306 | 0.719 | 7634543 |
| `V3R_c3fc027f9f` | C001 + C002 + C003 + C004 + C007 + C010 + C013 + C014 + C016 + C017 + C018 + C020 + C021 + C024 | H02 | HBENCH: tiebreak | 1.000 | 31.8 | 1015 | 1.301 | 0.744 | 7667889 |
| `V3R_0fbc71f963` | C001 + C002 + C003 + C004 + C007 + C011 + C014 + C016 + C017 + C018 + C020 + C021 + C024 | H04 | HBENCH: structural diversity (novas tags: table:C011) | 1.766 | 32.6 | 1029 | 1.276 | 0.719 | 6718219 |
| `V3R_63f05eeb86` | C001 + C002 + C003 + C004 + C005 + C008 + C010 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | HBENCH: structural diversity (novas tags: family:C2, family:C6, table:C005, table:C008) | 4.603 | 35.4 | 1693 | 1.181 | 0.535 | 8482953 |
| `V3R_f9e44778fb` | C001 + C002 + C003 + C004 + C007 + C010 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 1.000 | 31.8 | 1013 | 1.331 | 0.744 | 7895703 |
| `V3R_c4706867f2` | C001 + C002 + C003 + C004 + C005 + C007 + C010 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 1.008 | 31.8 | 1013 | 1.365 | 0.719 | 8508743 |
| `V3R_896c55893e` | C001 + C002 + C003 + C004 + C005 + C007 + C010 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 1.000 | 31.8 | 1012 | 1.391 | 0.744 | 8769903 |
| `V3R_e1f741583e` | C001 + C002 + C003 + C004 + C007 + C011 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 1.766 | 32.6 | 1026 | 1.306 | 0.719 | 6946033 |
| `V3R_0a6b5b4664` | C001 + C002 + C003 + C004 + C007 + C011 + C013 + C014 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 1.758 | 32.6 | 1027 | 1.301 | 0.744 | 6979379 |
| `V3R_9d1ecfdeb1` | C001 + C002 + C003 + C004 + C007 + C011 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 1.758 | 32.6 | 1025 | 1.331 | 0.744 | 7207193 |
| `V3R_36268b3c25` | C001 + C002 + C003 + C004 + C008 + C010 + C014 + C016 + C017 + C018 + C020 + C021 + C024 | H03 | PARETO, cortado pelo top_k após diversidade/desempate | 4.611 | 35.4 | 1697 | 1.067 | 0.510 | 7119779 |
| `V3R_17b9abdd15` | C001 + C002 + C003 + C004 + C008 + C010 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 4.611 | 35.4 | 1694 | 1.096 | 0.510 | 7347593 |
| `V3R_ec1bf31258` | C001 + C002 + C003 + C004 + C008 + C010 + C013 + C014 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 4.603 | 35.4 | 1695 | 1.092 | 0.535 | 7380939 |
| `V3R_e43b81b2f4` | C001 + C002 + C003 + C004 + C008 + C010 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 4.603 | 35.4 | 1693 | 1.121 | 0.535 | 7608753 |
| `V3R_22ae305593` | C001 + C002 + C003 + C004 + C005 + C008 + C010 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 4.611 | 35.4 | 1693 | 1.156 | 0.510 | 8221793 |
| `V3R_022b352807` | C001 + C002 + C003 + C004 + C008 + C011 + C014 + C016 + C017 + C018 + C020 + C021 + C024 | H05 | PARETO, cortado pelo top_k após diversidade/desempate | 5.369 | 36.2 | 1709 | 1.067 | 0.510 | 6431269 |
| `V3R_fb97fc7dd9` | C001 + C002 + C003 + C004 + C008 + C011 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 5.369 | 36.2 | 1707 | 1.096 | 0.510 | 6659083 |
| `V3R_5930611078` | C001 + C002 + C003 + C004 + C008 + C011 + C013 + C014 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 5.362 | 36.2 | 1708 | 1.092 | 0.535 | 6692429 |
| `V3R_0e92c048d2` | C001 + C002 + C003 + C004 + C008 + C011 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C024 |  | PARETO, cortado pelo top_k após diversidade/desempate | 5.362 | 36.2 | 1705 | 1.121 | 0.535 | 6920243 |
| `V3R_b5531e82be` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C016 + C017 + C018 + C020 + C021 + C023 + C024 |  | DOMINADO por V3R_cb2e537945 | 1.008 | 31.8 | 1017 | 1.306 | 0.719 | 7525001 |
| `V3R_d96487369a` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C016 + C017 + C018 + C019 + C020 + C021 + C024 |  | DOMINADO por V3R_cb2e537945 | 1.008 | 31.8 | 1017 | 1.336 | 0.719 | 7581569 |
| `V3R_fb749fe898` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C023 + C024 |  | DOMINADO por V3R_a82100f407 | 1.008 | 31.8 | 1014 | 1.335 | 0.719 | 7752815 |
| `V3R_5ebbb333ce` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C016 + C017 + C018 + C019 + C020 + C021 + C023 + C024 |  | DOMINADO por V3R_a82100f407 | 1.008 | 31.8 | 1017 | 1.365 | 0.719 | 7699841 |
| `V3R_1b5686c748` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C015 + C016 + C017 + C018 + C019 + C020 + C021 + C024 |  | DOMINADO por V3R_a82100f407 | 1.008 | 31.8 | 1014 | 1.365 | 0.719 | 7809383 |
| `V3R_c857e88dcf` | C001 + C002 + C003 + C004 + C007 + C010 + C013 + C014 + C016 + C017 + C018 + C020 + C021 + C023 + C024 |  | DOMINADO por V3R_c3fc027f9f | 1.000 | 31.8 | 1015 | 1.331 | 0.744 | 7786161 |
| `V3R_ef78a052ca` | C001 + C002 + C003 + C004 + C007 + C010 + C013 + C014 + C016 + C017 + C018 + C019 + C020 + C021 + C024 |  | DOMINADO por V3R_c3fc027f9f | 1.000 | 31.8 | 1015 | 1.361 | 0.744 | 7842729 |
| `V3R_a89be8f8bf` | C001 + C002 + C003 + C004 + C007 + C010 + C014 + C015 + C016 + C017 + C018 + C019 + C020 + C021 + C023 + C024 |  | DOMINADO por V3R_a82100f407 | 1.008 | 31.8 | 1014 | 1.395 | 0.719 | 7927655 |
| `V3R_70268aea62` | C001 + C002 + C003 + C004 + C005 + C007 + C010 + C014 + C016 + C017 + C018 + C020 + C021 + C024 |  | DOMINADO por V3R_a82100f407 | 1.008 | 31.8 | 1016 | 1.336 | 0.719 | 8280929 |
| `V3R_7329cfded8` | C001 + C002 + C003 + C004 + C007 + C010 + C013 + C014 + C015 + C016 + C017 + C018 + C020 + C021 + C023 + C024 |  | DOMINADO por V3R_f9e44778fb | 1.000 | 31.8 | 1013 | 1.360 | 0.744 | 8013975 |
| … | 226 deployments dominados omitidos (lista completa em `outputs_v3/rubis/prebenchmark/05_deployment_vectors.json`) | | | | | | | | |

Inviáveis: 32512 — motivos (contagem de ocorrências): C004 supersedes: 32768, C009 supersedes: 8192, dead table: 40960, no valid: 36864, write 16: 16384, write 28: 16384
- `H03` (só V2): PARETO, cortado pelo top_k após diversidade/desempate
- `H05` (só V2): PARETO, cortado pelo top_k após diversidade/desempate
