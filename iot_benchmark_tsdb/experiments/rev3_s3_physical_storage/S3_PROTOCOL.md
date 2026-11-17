# S3 — Physical Storage Controlled Experiment (campanha R1)

**Status:** protocolo congelado antes de qualquer DDL S3. Hash SHA-256 deste arquivo é gravado em `s3_preregister_draft.json` e em `s3_benchmark_preregister.json`; qualquer edição posterior invalida o pré-registro.
**Método:** `POST_AB_EXPLORATORY_SYNTHETIC_FIXED_HISTORY_DENSITY_SWEEP_FLUSHED_SSTABLE_STATE`, exploratório pós-A/B. Não é holdout confirmatório independente nem replay oficial do IoT Benchmark.

## 1. Pergunta

Com estado físico inicial comparável e verificado (todas as 12 tabelas persistidas em SSTables após `nodetool flush`, memtable vazia), a convergência da razão p95 TS2-D/TS2-U observada no S2 (+33% → +6,5% de 1× a 8×) se mantém?

Hipóteses (pré-registradas, não resultados):

- H1: persistir os dados em SSTables muda níveis e/ou razões de p95 em relação ao S2, que rodou majoritariamente em memtables.
- H2: U mantém menos chamadas CQL em janelas longas; D tem mais partições menores (fan-out de buckets).
- H3: maior densidade reduz o peso relativo do custo fixo por requisição, sem garantia de crossover.
- H4: tamanho de partição em disco e fan-out de buckets explicam parte do trade-off e são candidatos a proxies pré-benchmark (a validar só em campanha futura, sem leakage).

## 2. Invariantes (iguais ao S2)

| Item | Valor |
|---|---|
| Seleção Rev3 B | `8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231` (não reexecutada) |
| Variantes | `BASELINE_Q8_OPTIMIZED`, `R3H_38939db96444` (TS2-U), `R3H_d2ea1974de53` (TS2-D) |
| Dados | CSVs S2 existentes, só leitura: 1× `047c269a…`, 2× `7389fcdf…`, 4× `b5aa8eb7…`, 8× `dcdbcf01…` |
| Casos | `s1_cases.json` `31fd36c6…`: 18 casos (Q2/Q7/Q9/Q12 × 1/7/14/21 dias + Q5/Q8) |
| Executores | `rev3_confirmatory.benchmark.baseline_dispatch`, `rev3.runtime.dispatch`, `source_oracle`, `offline.compare` do B congelado |
| Materializador | `iot_pipeline.stage9._materialize` (TS2) e `v4_rev2.runtime_helpers.load_wide` (baseline), como no S2 |
| Medição | 5 warm-ups + 60 rodadas por (caso, variante); `page_size=16`; `perf_counter_ns` ponta a ponta no cliente; oráculo fora da região cronometrada; p95 por posto mais próximo (57ª de 60) |
| Seed | `20261127`; RNG por fator `Random(20261127 + fator)`, **idêntico ao S2**, para que a sequência de casos e variantes seja a mesma e a comparação S2×S3 fique pareada por ordem |
| Ordem de fatores | 1× → 2× → 4× → 8× (mesma ordem do S2) |
| Cassandra | container `schemalens_iot_cassandra_pipeline-cassandra-1`, 4.1.12, `127.0.0.1:9043`, nó único, `SimpleStrategy` RF=1, DDL congelado (LZ4, STCS, opções padrão) |

## 3. Tratamento (a única variável nova)

Estado físico inicial: **todas as tabelas S3 descarregadas em SSTables** por `nodetool flush <keyspace_s3>` antes da validação e da medição.

Não são aplicados: compactação manual, limpeza de cache, mudança de compaction/compression, TTL, outro driver, desativação de autocompaction. Flush não equivale a cache frio: page cache do SO, key cache do Cassandra e compactações automáticas em background continuam possíveis e são registrados.

## 4. Keyspaces (somente novos)

`sl_s3_r1_d{1,2,4,8}_{ref,u,d}` (12 keyspaces). Regex aceita: `sl_s3_r1_d(?:1|2|4|8)_(?:ref|u|d)`.
Regras: `CREATE KEYSPACE` sem `IF NOT EXISTS`; aborta antes do primeiro CREATE se qualquer nome já existir; nunca `DROP`, `TRUNCATE`, `ALTER`, `UPDATE` ou `DELETE`; nunca `_create_keyspace()` nem `stage9.run_live()`; falha parcial é preservada, sem limpeza automática. `nodetool` restrito a uma lista de comandos permitidos; `flush` só aceita keyspaces S3 e nunca roda sem keyspace.

## 5. Estratégia A (escolhida antes da medição)

1. Preflight somente leitura (`preflight/`).
2. Materializar os 12 keyspaces (1×, 2×, 4×, 8×).
3. Flush de cada keyspace S3, com espera por quiescência de compactação (`pending tasks: 0` em 3 leituras consecutivas, intervalo de 5 s, máximo de 600 s; sem compactação manual).
4. Snapshot `storage_post_flush` com gate: em cada tabela, `SSTable count ≥ 1`, `Space used (live) > 0` e `Memtable cell count = 0`. Se falhar, a campanha **para**.
5. Validação semântica 216/216 contra o oráculo. Os traços (`execute_calls`, `iterated_rows`, `physical_plan`) precisam ser **idênticos** aos do S2.
6. Pré-registro final (hashes de código, protocolo, dados, manifests, snapshots de armazenamento).
7. Para cada fator na ordem: snapshot `storage_pre_timing` com o mesmo gate; benchmark (3.240 medições); snapshot `storage_post_timing`, que registra se o estado mudou durante a medição (novas SSTables, memtable não vazia).
8. Auditoria por fator, auditoria completa (12.960 medições, 4.320 ordens), comparação S3×S2 e relatório.

## 6. Critérios de parada

Para imediatamente, preservando evidências (`failure_manifest.json`, CSVs parciais), em caso de: divergência semântica; traço de acesso diferente do registrado; gate de armazenamento reprovado; mudança de hash após o pré-registro; keyspace S3 preexistente; versão do Cassandra diferente de 4.1.12. Uma nova tentativa usa **nova réplica** (`r2`), nunca sobrescreve R1.

## 7. Análise pré-especificada

- Por fator: vitórias U/D nas 16 combinações temporais; mediana de `p95_D/p95_U`; Q5/Q8 reportados à parte.
- S3×S2 pareado por (fator, caso, variante): razão `p95_S3/p95_S2`; mudança da razão D/U; número de vencedores invertidos.
- Armazenamento: SSTable count, live bytes, tamanho médio e máximo de partição compactada, partições estimadas, memtable antes e depois da medição.
- Leitura descritiva; sem alegação de significância. Uma única réplica R1 não sustenta inferência sobre variabilidade entre campanhas.

## 8. Ameaças à validade (específicas do S3)

- **Confundimento temporal:** S2 foi medido em 05/11 e S3 em 10/11, em outro momento do host (outros containers ativos). Diferenças S3×S2 misturam estado físico, tempo e carga do sistema.
- **Page cache quente:** as SSTables acabaram de ser escritas e provavelmente estão no page cache do SO. S3 mede "SSTable com cache quente", não disco frio.
- **Estado do S2 alterado após a medição:** o container parou de forma não limpa em 10/11 14:52 UTC (exit 255). O replay do commitlog no reinício descarregou as memtables S2 em SSTables. As evidências medidas do S2 não mudam, mas os keyspaces S2 não estão mais no estado em que foram medidos (registrado em `preflight/`).
- **Contagem de SSTables pode diferir entre variantes** (flushes automáticos durante a carga). O gate exige ≥ 1; as contagens exatas são reportadas.
- Ameaças herdadas: dados sintéticos derivados, histórico de ~20,83 dias, 9 dispositivos e 2 sensores, nó único, latência medida no cliente Python, p95 de 60 amostras dependentes e múltiplas comparações.
