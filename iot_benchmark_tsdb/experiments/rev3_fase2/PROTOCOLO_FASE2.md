# SchemaLens Rev3 — Fase 2 (E0–E6): robustez e alcance

**Status:** protocolo congelado antes de qualquer DDL da Fase 2. O SHA-256 deste arquivo entra em cada pré-registro de campanha; qualquer edição posterior invalida as campanhas ainda não medidas.
**Escopo:** pós-A/B, exploratório quanto a E1; E3/E4 funcionam como **holdout** para a Rev4, porque a Rev4 é congelada antes de qualquer latência de E3/E4.

## Ordem de execução (e por quê)

| Ordem | Etapa | Pergunta | Depende de |
|---:|---|---|---|
| 0 | E0 — Empacotar S3 R1 | Evidência S3 disponível para revisão independente | — |
| 1 | E1 — Réplicas r2 e r3, memtable × SSTable na mesma sessão | O resultado do S3 se repete? Qual é o efeito isolado do flush? | dados S2 (só leitura) |
| 2 | E2 — Rev4: calibrar e congelar o preditor U×D | Dá para prever o vencedor antes de medir? | S2, S3, E1 (desenvolvimento) |
| 3 | E3 — Histórico real Java: H1 (21 d), H2 (83 d), H3 (333 d) | Partições maiores fazem o bucket diário (D) compensar? | dados Java novos; Rev4 congelada |
| 4 | E4 — Número de dispositivos: N3, (H1 = 9), N18 | O número de dispositivos muda o vencedor? | dados Java novos; Rev4 congelada |
| 5 | E5 — Avaliar a Rev4 no holdout E3+E4 | A Rev4 acerta fora dos dados de calibração? | E2, E3, E4 |
| 6 | E6 — Cache frio (evicção best-effort) | Quanto muda quando os dados não estão no page cache? | E3 |

A Rev4 (E2) vem **antes** de E3/E4 de propósito: as previsões são gravadas com hash depois da validação semântica e do snapshot físico (que só usam dados e estrutura, nunca latência) e antes da primeira medição. Assim E3/E4 são holdout verdadeiro para a Rev4.

## Invariantes (iguais a S2/S3)

- Seleção Rev3 B congelada `8f886938…`; variantes `BASELINE_Q8_OPTIMIZED` (REF), `R3H_38939db96444` (TS2-U), `R3H_d2ea1974de53` (TS2-D); planos fixos, **sem nova seleção** (experimento de sensibilidade de planos fixos, como previsto no `S2_PLANO_PT.md`).
- Executores, oráculo, comparação, materializador e `page_size=16` do B congelado (reusados via `s3_physical.py`, que não é modificado).
- 5 warm-ups; p95 por posto mais próximo; `perf_counter_ns` ponta a ponta no cliente; oráculo fora da região cronometrada; traço de acesso (`execute_calls`, `iterated_rows`, `physical_plan`) conferido em toda execução.
- Cassandra 4.1.12, container `schemalens_iot_cassandra_pipeline-cassandra-1`, `127.0.0.1:9043`, nó único, RF=1, DDL congelado.

## Segurança

Somente keyspaces novos `sl_f2_*`; `CREATE KEYSPACE` sem `IF NOT EXISTS`; nunca `DROP/TRUNCATE/ALTER/UPDATE/DELETE`; `nodetool flush` só em keyspaces `sl_f2_*_sst_*`; `nodetool invalidatekeycache` só em E6; arquivos de evidência abertos com modo `x` (sem sobrescrita); falha preserva evidência e nova tentativa usa novo nome de campanha.

## E1 — Réplicas pareadas (r2, r3)

- Dados: CSVs S2 1×, 2×, 4×, 8× (hashes verificados). 18 casos S1.
- Para cada réplica e fator: 6 keyspaces novos = {mem, sst} × {ref, u, d}. `sst` recebe `nodetool flush` + espera de compactação; `mem` nunca é descarregado.
- Gate antes da medição: `sst` → SSTable ≥ 1, memtable = 0; `mem` → SSTable = 0, memtable > 0. Reprovação para a campanha.
- Medição: 60 rodadas; em cada rodada a ordem dos casos é embaralhada e, por caso, a ordem dos **6 braços** (condição × variante) é embaralhada e invertida em rodadas pares. Sementes: r2 = 20261211, r3 = 20261223 (`Random(seed + fator)`).
- Saídas: p95 por (caso, condição, variante); efeito do flush pareado na mesma sessão = p95_sst / p95_mem; mediana D/U por condição; comparação com S2 (mem) e S3 R1 (sst).

## E2 — Rev4

- Desenvolvimento: casos temporais (Q2/Q7/Q9/Q12) de S2, S3 R1 e E1 r2/r3, variantes U e D.
- Modelo pré-especificado: `ln(p95) = θ0 + θ1·ln(calls) + θ2·ln(rows+1) + θ3·ln(partição média em bytes) + θ4·[sst]`, mínimos quadrados, ajustado uma vez.
- Variáveis de entrada disponíveis antes de medir: `execute_calls` e `iterated_rows` (validação semântica) e partição média (tablestats pós-flush).
- Congelamento: coeficientes + hash do código em `rev4/rev4_frozen.json`. Previsões de E3/E4 em `rev4/predictions_<campanha>.json`, gravadas antes da primeira medição da campanha e incluídas no pré-registro.
- Linhas de base comparadas em E5: (a) Rev3 "sempre U" (o que a seleção B implica); (b) regra estrutural "menos chamadas vence; empate → menor partição".

## E3 — Histórico real (holdout)

- Dados Java novos (gerador oficial, commit `62e0dc10…`, sementes inéditas 5412xx–5413xx), 9 dispositivos, 2 sensores, `POINT_STEP` 10 min: H1 = 600 loops (20,8 d), H2 = 2.400 (83,3 d), H3 = 9.600 (333,3 d). Controle: o mesmo caminho reproduziu byte a byte os dados do B.
- Casos: os 18 casos S1 (âncora 01/01/2024) + extensões determinísticas de janela: W80 (H2, H3) e W330 (H3) para Q2/Q7/Q9/Q12.
- Estado: `sst` (flush + gate), 60 rodadas, semente 20261301.

## E4 — Dispositivos (holdout)

- N3 (3 dispositivos) e N18 (18), 600 loops; H1 é o ponto de 9 dispositivos. Em N3 os dispositivos dos casos são remapeados `d_k → d_(k mod 3)` (determinístico, registrado).
- Estado `sst`, 60 rodadas, semente 20261401.

## E5 — Avaliação da Rev4

Acerto do vencedor U×D por caso temporal, erro absoluto mediano de `ln(D/U)` previsto × medido, comparados com as duas linhas de base. Leitura descritiva.

## E6 — Cache frio (best-effort)

- Keyspaces de E3 (H1 e H3). Braços = variante × {quente, frio}. Antes de cada execução "fria": `posix_fadvise(DONTNEED)` em todos os arquivos do keyspace (dentro do container) e `nodetool invalidatekeycache`, fora da região cronometrada. 30 rodadas, semente 20261601.
- Limitação conhecida e medida: páginas mapeadas por mmap pelo processo do Cassandra não são liberadas; a residência (`fincore`) é registrada. E6 mede "cache parcialmente frio", não disco frio. Cache frio real exigiria reiniciar o Cassandra e root no host, fora deste protocolo.

## Ameaças à validade

Nó único; latência no cliente Python; p95 de 60 (ou 30) amostras dependentes; múltiplas comparações; E1 usa dados sintéticos densificados (S2); E3/E4 usam planos fixos do B (sem nova seleção por escala); sem alegação de significância estatística.
