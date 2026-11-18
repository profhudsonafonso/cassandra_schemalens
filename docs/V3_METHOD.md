# V3 — Método (Unified Stage 7 / Stage 8)

## Pergunta científica

A V3 **não** tenta prever qual deployment será mais rápido. Ela pergunta se um Stage 7 único, baseado apenas em
**trabalho físico observável antes do benchmark**, consegue reduzir o espaço de projeto e **preservar** as alternativas
empiricamente competitivas, sem coeficientes de latência específicos de dataset. Kendall τ é diagnóstico retrospectivo.

## Arquitetura (igual nos quatro benchmarks)

```
IoT ─────┐
TPCx ────┤   adaptadores (só convertem artefatos;     unified_stage7_v3/adapters/{iot,tpcx_iot,tsbs,rubis}.py
TSBS ────┼── nenhum peso, nenhum fator de família)
RUBiS ───┘
            ↓  CandidateSpec, QueryPlan(AccessStep), WritePlan, Workload   (model.py)
     UnifiedStage7Estimator  → ReadWork / WriteWork / Footprint            (estimator.py, logical_bytes.py)
            ↓
     agregação por deployment → PhysicalWorkVector                         (aggregation.py)
            ↓  cobertura (toda leitura tem rota) + viabilidade (escritas mantíveis, fonte canônica, sem tabela morta)
     Pareto universal (6 eixos)                                            (pareto.py)
            ↓  |Pareto| ≤ top_k → mantém tudo; senão diversidade estrutural → balanced_structural_tiebreak
     HBench V3                                                             (selection.py)
            ↓  SHA-256 de código, entradas, saídas 00–11, cenário, protocolo e harness; pré-registro
     freeze (ABORT se qualquer hash mudar)                                 (freeze.py, plans.py)
            ↓
     Cassandra 4.1.12 (protocolo comum v1)                                 (stage9/)
            ↓
     resultado medido                                                      (analysis/)
```

## Estimativa estrutural de trabalho (Stage 7 V3)

* **Leitura** (por execução de um QueryPlan): `read_cql_requests`, `partitions_touched`, `logical_rows`,
  `logical_bytes` (proxy), `bucket_or_partition_fanout` (partições ÷ operandos lógicos).
* **Escrita** (por execução de uma escrita, por tabela mantida): `cql_mutations`, `rows_touched`,
  `maintenance_read_requests`, `maintenance_rows_read`, `logical_bytes_written`. Leituras de suporte compartilhadas
  por todas as tabelas de uma escrita contam uma vez por (escrita, entidade).
* **Footprint** (por CandidateSpec): `logical_storage_bytes` (proxy), `physical_partition_count`,
  `max_partition_rows`, `max_partition_bytes`, `duplication_ratio`.
* **Logical byte proxy LBP-1** (`policy.json`): larguras fixas por tipo CQL; tipos variáveis usam a largura média
  declarada ou estimada nos dados pré-benchmark da campanha; sem largura ⇒ `UNKNOWN` (nunca zero). Não são bytes de
  disco, rede ou SSTable.
* **Sem escore escalar, sem latência, sem NNLS, sem fator por família** (testes 1, 3, 8, 15).

## Cenário = campanha executada (D-V3-003)

| Benchmark | Cenário V3 (`outputs_v3/<b>/V3_PREBENCHMARK_SCENARIO.json`) | Cenário antigo descartado |
|---|---|---|
| IoT | campanha B Rev3: mesmo CSV e mesmos 12 objetos de consulta medidos | — |
| TPCx | 2 estações × 3 sensores, 130 + 500 passos, 120 leituras/execução, RF 1, cronograma de sondas congelado | 10 × 200, 1 dia, RF 2 |
| TSBS | gerador oficial: 40 hosts, 600 s, 36 h, 86 400 células; 96 instâncias congeladas | 100 hosts, 60 s, 3 dias |
| RUBiS | estado da coorte congelada (V2) com estatísticas E04 recalculadas; fan-outs no domínio de parâmetros das histórias | escala completa (2 M itens, 20 M lances) |

## Frequências

IoT `EXPERIMENTAL_POLICY_UNIFORM`; TPCx parâmetros congelados (120 leituras e 3 780 eventos por execução);
TSBS `UNSPECIFIED` + `UNIFORM_QUERY_CLASS`; RUBiS E05, média das 4 mixes (D-V3-009).

## Stage 8 V3

1. Enumeração exata do espaço (IoT 6, TPCx 9, TSBS 256, RUBiS 32 768 subconjuntos) — algoritmo pode variar, a semântica não.
2. Rota por operação dentro do deployment: mínimo lexicográfico (requisições, linhas, bytes, plan_id), apenas entre planos
   válidos (guardas de execução, p. ex. alinhamento do TSBS, avaliados por instância).
3. Viabilidade: rota para toda leitura; toda tabela afetada mantível; suporte de manutenção presente; fonte canônica;
   regra estrutural universal sem tabela morta (D-V3-007); regras de admissibilidade estruturais herdadas (RUBiS
   `supersedes` e cópia completa D-V9-010).
4. Pareto (minimização) nos eixos `read_cql_requests, read_logical_rows, read_logical_bytes, write_cql_mutations,
   maintenance_read_requests, logical_storage_bytes`; valor desconhecido bloqueia dominância. Eixos de risco ficam
   como diagnóstico (D-V3-006).
5. Se |Pareto| ≤ top_k (=5) mantém toda a fronteira; senão diversidade estrutural gulosa (fonte canônica, tabela,
   combinação de famílias, bucket, projeção × nenhuma, bruto × agregado) e desempate por
   `balanced_structural_tiebreak` = média, com pesos iguais, de valor ÷ menor valor positivo do eixo no espaço
   pré-benchmark. Usado só para ordenar/desempatar; não é latência prevista.

## Firewall e congelamento

Todo Stage 7/8 roda sob `guards.firewall()`, que intercepta leituras de arquivo e aborta em padrões de resultados
medidos; manifests registram `selection_uses_measured_results=false`, `latency_files_read=[]`,
`benchmark_outputs_read=[]` e a lista de arquivos abertos. O freeze reexecuta o Stage 7/8 em processo novo e exige
saídas idênticas byte a byte; todo ponto de entrada do Stage 9 chama `verify()` (ABORT em qualquer diferença).

## Medição (Stage 9 V3)

Regra do usuário (D-V3-001): mede-se de novo apenas quando o conjunto a medir contém algum (tabelas, rotas) não medido
na V2 sob o protocolo v1. IoT e TPCx: medições V2 reutilizadas (verificação completa, rotas idênticas). TSBS:
verificação completa dos 24 deployments V3 + baseline original + A/A. RUBiS: HBench V3 + NoSE + QPG +
V8_AUTO_H_norm + A/A em 4.1.12 (principal) e 3.11.11 (secundária), em nós novos. Protocolo v1 inalterado
(`cassandra@sha256:25f6a4d5…`, RF 1, 5 repetições a frio, gate de flush, ordem contrabalançada, oráculo, bootstrap
pareado, piso A/A, armazenamento, guarda de isolamento).

## Reprodução

```
./run_v3_unified.sh --audit
./run_v3_unified.sh --prebenchmark      # Stage 7/8 V3 + docs/V3_PREBENCHMARK_COMPARISON.md
./run_v3_unified.sh --test              # 15 testes obrigatórios (41 casos)
./run_v3_unified.sh --freeze            # SHA-256 + PREREGISTRATION_V3.json por benchmark
./run_v3_unified.sh --benchmark         # reuso IoT/TPCx; TSBS e RUBiS desacoplados
./run_v3_unified.sh --status
./run_v3_unified.sh --analyze           # métricas, ablações, NNLS retrospectivo, docs/V3_RESULTS_COMPARISON.md
```
