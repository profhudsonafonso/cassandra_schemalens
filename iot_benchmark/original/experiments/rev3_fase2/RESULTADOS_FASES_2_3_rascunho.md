# SchemaLens Rev3 — Fases 2 e 3: rascunho de resultados

_Gerado automaticamente por `fase3_analysis.py draft` a partir dos JSON auditados; nenhum número foi digitado._

## Métodos (resumo)

Todas as campanhas usaram keyspaces novos, gate de estado físico antes e depois da medição, oráculo semântico em toda execução, 5 aquecimentos e 60 rodadas aleatorizadas por caso e variante (30 em E6), p95 por posto mais próximo e auditoria que recalcula as estatísticas a partir das latências brutas. Protocolo e emendas 01–04 em `experiments/rev3_fase2/`.

## Réplicas e efeito do armazenamento (E1, E7)

- Mediana D/U em SSTable no 1× → 8×: r1 1,282 → 1,095; r2 1,505 → 1,129; r3 1,319 → 1,136.
- Mesmo vencedor U/D nas três réplicas SSTable (r1, r2, r3) em 47 de 64 casos temporais.
- Vitórias em SSTable somando r2 e r3: TS2-U 100, TS2-D 28 (de 128).
- Efeito isolado do flush no 8× (SSTable/memtable, mesma sessão): REF +14.1%, U +15.5%, D +15.6%; a diferença S3/S2 (dias diferentes) era REF +2.8%, U +11.3%, D +14.7%.
- Mesmo vencedor U/D nas duas réplicas em 71 de 92 casos temporais.
- Diferença típica de p95 entre réplicas (mediana de |ln r2/r1|): REF 5.3%, U 5.5%, D 8.3%.
- H1 · 21 dias · 9 disp.: mediana D/U 1,306 (r1) e 1,296 (r2); U venceu 14 e 12 de 16.
- H2 · 83 dias · 9 disp.: mediana D/U 1,372 (r1) e 1,364 (r2); U venceu 18 e 16 de 20.
- H3 · 333 dias · 9 disp.: mediana D/U 1,354 (r1) e 1,389 (r2); U venceu 19 e 20 de 24.
- N3 · 21 dias · 3 disp.: mediana D/U 1,518 (r1) e 1,489 (r2); U venceu 12 e 14 de 16.
- N18 · 21 dias · 18 disp.: mediana D/U 1,414 (r1) e 1,377 (r2); U venceu 13 e 13 de 16.

## Histórico, dispositivos e janelas (E3, E4, E9)

- Holdout: TS2-U venceu 76 e TS2-D 16 de 92 casos temporais.
- Acerto do vencedor: Rev4 76/92, 'sempre U' 76/92, 'menos chamadas' 76/92.
- Erro absoluto mediano de ln(D/U) da Rev4: 0.138 (≈ ×1.15 na razão).
- Partição média de U: 103 KB (21 d) → 1874 KB (333 d); mediana D/U 1,306 → 1,354.
- Janelas longas (80 e 330 dias): mediana D/U 2,060; D venceu 0 de 12.
- Holdout E9: TS2-U venceu 61 e TS2-D 19 de 80 casos temporais.
- Partições grandes (P1): partição de U 16.6 MB (máx. 16.6 MB) contra 50 KB de D; mediana D/U 1,129; D venceu 4 de 20.

## Preditores pré-benchmark (Rev4, Rev5)

- Rev4 (log-linear): 768 observações de desenvolvimento, R² 0.9849, coeficientes {'intercept': -4.735409, 'ln_calls': 0.116509, 'ln_rows_plus_1': 0.929458, 'ln_partition_mean_bytes': 0.014473, 'sst': 0.106919}.
- Rev5 (aditivo): 752 observações, erro relativo mediano 0.1189, coeficientes {'b0_fixed_ms': -0.22237784, 'b1_per_call_ms': 0.191788142, 'b2_per_call_per_ln_partition_ms': 0.043062132, 'b3_per_row_ms': 0.006276809}.
- Acerto do vencedor: Rev4 76/92, 'sempre U' 76/92, 'menos chamadas' 76/92.
- Erro absoluto mediano de ln(D/U) da Rev4: 0.138 (≈ ×1.15 na razão).
- Acerto do vencedor: Rev5 59/80, Rev4 59/80, 'sempre U' 61/80, 'menos chamadas' 59/80.
- Erro mediano de ln(D/U): Rev5 0.119 (×1.13) contra Rev4 0.171 (×1.19).

## Decomposição do tempo (E10, tracing)

- h3 Q12_W01 TS2-D: 2 chamada(s), 0 requisições ao servidor, 3.522 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W01 TS2-U: 2 chamada(s), 0 requisições ao servidor, 3.002 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W07 TS2-D: 14 chamada(s), 0 requisições ao servidor, 14.031 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W07 TS2-U: 2 chamada(s), 0 requisições ao servidor, 8.002 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W21 TS2-D: 42 chamada(s), 0 requisições ao servidor, 67.526 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W21 TS2-U: 2 chamada(s), 0 requisições ao servidor, 39.455 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W80 TS2-D: 42 chamada(s), 0 requisições ao servidor, 57.548 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q12_W80 TS2-U: 2 chamada(s), 0 requisições ao servidor, 39.919 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W01 TS2-D: 1 chamada(s), 0 requisições ao servidor, 1.33 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W01 TS2-U: 1 chamada(s), 0 requisições ao servidor, 1.531 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W07 TS2-D: 7 chamada(s), 0 requisições ao servidor, 7.307 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W07 TS2-U: 1 chamada(s), 0 requisições ao servidor, 4.415 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W21 TS2-D: 21 chamada(s), 0 requisições ao servidor, 22.871 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W21 TS2-U: 1 chamada(s), 0 requisições ao servidor, 8.75 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W80 TS2-D: 80 chamada(s), 0 requisições ao servidor, 118.358 ms no cliente, 0.0 ms no servidor (fração 0.0).
- h3 Q2_W80 TS2-U: 1 chamada(s), 0 requisições ao servidor, 53.656 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q12_W01 TS2-D: 2 chamada(s), 0 requisições ao servidor, 7.707 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q12_W01 TS2-U: 2 chamada(s), 0 requisições ao servidor, 7.667 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q12_W07 TS2-D: 14 chamada(s), 0 requisições ao servidor, 76.62 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q12_W07 TS2-U: 2 chamada(s), 0 requisições ao servidor, 68.175 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q12_W21 TS2-D: 42 chamada(s), 0 requisições ao servidor, 263.198 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q12_W21 TS2-U: 2 chamada(s), 0 requisições ao servidor, 199.577 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q2_W01 TS2-D: 1 chamada(s), 0 requisições ao servidor, 5.245 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q2_W01 TS2-U: 1 chamada(s), 0 requisições ao servidor, 6.149 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q2_W07 TS2-D: 7 chamada(s), 0 requisições ao servidor, 61.564 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q2_W07 TS2-U: 1 chamada(s), 0 requisições ao servidor, 56.585 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q2_W21 TS2-D: 21 chamada(s), 0 requisições ao servidor, 188.206 ms no cliente, 0.0 ms no servidor (fração 0.0).
- p1 Q2_W21 TS2-U: 1 chamada(s), 0 requisições ao servidor, 188.9 ms no cliente, 0.0 ms no servidor (fração 0.0).

## Cache frio (E6)

- H1 · 21 dias · 9 disp.: frio/quente U +6%, D +12%; D/U 1,494 quente → 1,469 frio; D venceu 3/16 no frio; 93% dos bytes continuavam no page cache após a evicção (mmap).
- H3 · 333 dias · 9 disp.: frio/quente U +22%, D +8%; D/U 1,718 quente → 1,641 frio; D venceu 3/24 no frio; 48% dos bytes continuavam no page cache após a evicção (mmap).

## Limitações

- Nó único; latência medida no cliente Python; p95 de 60 amostras; sem teste de significância.
- Planos fixos da seleção B; consultas derivadas de objetos Java, não replay do mix oficial.
- E6 é cache parcialmente frio (mmap); cache frio real não foi executado (aguarda autorização).
- O tracing (E10) acrescenta custo; serve para decompor o tempo, não como latência de referência.

## Fase 4 (EMENDA_05): regra com zona de empate, réplica de E9 e tracing do P1

_Seção acrescentada por `fase4.py e18` a partir dos JSON auditados._

- E10 corrigido: a associação de traces usava hora local em vez de UTC; reassociação registrada em E13.
- EXPLORATÓRIO (E9 já visto): T5 decidiu firme em 61 de 80 casos e acertou 48; erros graves 8; 19 declarados empate.
- Arrependimento p90: T5 0.119, sempre U 0.119, Rev5 0.293, Rev4 0.293.
- Casos com empate medido (|ln D/U| < 0.1): 17 de 80.
- Teste confirmatório da regra T5 em e9r2: 0 de 4 hipóteses atendidas (H5.1 ✘, H5.2 ✘, H5.3 ✘, H5.4 ✘).
- T5: decisão firme em 61 de 80 casos, acerto firme 47/61, erros graves 7; arrependimento p90 0.101 (sempre U 0.075).
- Réplica E9 × e9r2: mesmo vencedor em 59 de 80 casos temporais.
- Anomalia P1 janela de 1 dia: mediana D/U em e9r2 = 1.016 → não reproduzida (critério EMENDA_05: ≤1,10 não; ≥1,30 sim).
- E16 e9/p1 Q12_W01 TS2-D: cliente mediana 22.291 ms, p95 41.876 ms; servidor 5.671 ms em 2.0 requisições.
- E16 e9/p1 Q12_W01 TS2-U: cliente mediana 21.525 ms, p95 41.377 ms; servidor 5.797 ms em 2.0 requisições.
- E16 e9/p1 Q12_W07 TS2-D: cliente mediana 101.167 ms, p95 123.193 ms; servidor 24.755 ms em 14.0 requisições.
- E16 e9/p1 Q12_W07 TS2-U: cliente mediana 92.68 ms, p95 106.313 ms; servidor 23.756 ms em 4.0 requisições.
- E16 e9/p1 Q2_W01 TS2-D: cliente mediana 15.551 ms, p95 16.671 ms; servidor 5.465 ms em 1.0 requisições.
- E16 e9/p1 Q2_W01 TS2-U: cliente mediana 15.619 ms, p95 23.961 ms; servidor 5.803 ms em 1.0 requisições.
- E16 e9/p1 Q2_W07 TS2-D: cliente mediana 88.669 ms, p95 108.91 ms; servidor 24.602 ms em 7.0 requisições.
- E16 e9/p1 Q2_W07 TS2-U: cliente mediana 80.428 ms, p95 98.566 ms; servidor 26.612 ms em 3.0 requisições.
- E16 e9r2/p1 Q12_W01 TS2-D: cliente mediana 22.515 ms, p95 23.531 ms; servidor 5.713 ms em 2.0 requisições.
- E16 e9r2/p1 Q12_W01 TS2-U: cliente mediana 22.373 ms, p95 42.328 ms; servidor 5.96 ms em 2.0 requisições.
- E16 e9r2/p1 Q12_W07 TS2-D: cliente mediana 103.18 ms, p95 127.5 ms; servidor 28.91 ms em 14.0 requisições.
- E16 e9r2/p1 Q12_W07 TS2-U: cliente mediana 87.046 ms, p95 109.957 ms; servidor 24.003 ms em 4.0 requisições.
- E16 e9r2/p1 Q2_W01 TS2-D: cliente mediana 16.072 ms, p95 35.515 ms; servidor 5.638 ms em 1.0 requisições.
- E16 e9r2/p1 Q2_W01 TS2-U: cliente mediana 15.952 ms, p95 18.485 ms; servidor 6.005 ms em 1.0 requisições.
- E16 e9r2/p1 Q2_W07 TS2-D: cliente mediana 71.927 ms, p95 106.81 ms; servidor 16.378 ms em 7.0 requisições.
- E16 e9r2/p1 Q2_W07 TS2-U: cliente mediana 79.606 ms, p95 98.575 ms; servidor 27.473 ms em 3.0 requisições.
