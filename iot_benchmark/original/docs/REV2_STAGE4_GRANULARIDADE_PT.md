# SchemaLens IoT/Cassandra — Revisão do Stage 4 (Rev2)

**Escopo:** revisão do design space físico, mantendo **TS0–TS8** e regras originais de ativação **inalteradas**. Implementação Python com contratos revisados nos Stages 4–9. **Não há nova medição Cassandra neste pacote.**

## 1. Por que revisar?

O Pilot Experiment 1 apresentou uma baseline Cassandra que traduz a organização lógica `table` do IoTDB: `(device_id, timestamp_ms)` com diversos sensores em colunas. O Stage 4 do protótipo tinha uma representação TS2 *wide* apenas com `(device_id,bucket_day)` como partition key. A diferença física existe independentemente de qual candidato for mais rápido. É justificável estudar granularidades como **alternativas físicas da mesma família TS2**: nenhuma regra de ativação foi alterada e nenhuma família nova foi inventada.

**Atenção:** esta revisão foi motivada por inspeção *após* observar o piloto anterior, portanto é uma **hipótese de projeto pós-piloto**. Não podemos usar a melhoria sobre o mesmo piloto como evidência confirmatória independente. A avaliação empírica de Rev2 deverá usar outro conjunto de dados/workload, devidamente controlado. As latências já medidas não fazem parte do algoritmo de seleção.

## 2. Duas instanciações da família TS2

### TS2-D — Bucket diário

```sql
CREATE TABLE IF NOT EXISTS ts2_iot_wide (
  device_id text, bucket_day date, timestamp_ms bigint,
  m_s0 double, m_s1 double,
  PRIMARY KEY ((device_id,bucket_day), timestamp_ms)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC);
```

É a instanciação do Stage 4 anterior. Uma consulta sobre vários dias precisa enumerar as partições UTC correspondentes.

### TS2-U — Sem bucket

```sql
CREATE TABLE IF NOT EXISTS ts2_iot_wide_unbucketed (
  device_id text, timestamp_ms bigint,
  m_s0 double, m_s1 double,
  PRIMARY KEY ((device_id), timestamp_ms)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC);
```

Uma consulta por dispositivo evita enumerar dias, **mas** a partição pode crescer indefinidamente. A política `max_unbucketed_rows_per_device = 100000` é uma **restrição demonstrativa pré-declarada**, aplicada ao número de observações do dataset por dispositivo, e deve ser substituída por estimativas de cardinalidade/retention fundamentadas para experimentos reais. Ela não é um limite oficial do Cassandra nem garantia de ausência de partições grandes. Não usar o resultado p95 para ajustar esse valor.

A1 gera os nomes e tipos dos sensores; a instanciação de TS2-U reutiliza as colunas do blueprint A1. A família TS2 continua dependente de evidência de ativação pelo workload. Para a variante diária, também é exigida a presença de evidência do modificador **M1** em alguma consulta ativando TS2 na workload. Não alteramos a regra do M1 original; apenas consumimos seu resultado.

## 3. Propagação entre os estágios

| Stage | Rev2 | Evidência de teste |
|---|---|---|
| 4 | Gera TS2-D e TS2-U como candidatos distintos, sob TS2 | DDL/guard/M1/capacidade |
| 5 | Mantém `candidate_id` para distinguir duas rotas físicas | 32 ligações ativadas; coverage estrutural |
| 6 | Gera WritePlan de TS2-U separadamente | `INSERT` distinto, sem bucket |
| 7 | Usa contagem pré-benchmark de requisições: TS2-U por dispositivo, TS2-D por dispositivo/dia | Nenhuma latência medida entra na estimativa |
| 8 | Consolida por candidate ID; variantes TS2 são mutuamente exclusivas no mesmo deployment | 24 combinações completas admitidas; cinco selecionadas |
| 9 | CQL separado para TS2-U e TS2-D em leitura e carga | Testes com fake session; **não executado em Cassandra aqui** |

**Manutenção:** ambas as variantes usam a TS0 como fonte canônica no projeto atual. As limitações previamente declaradas de carga batch, correções e escrita concorrente continuam existindo.

## 4. Execução offline

```bash
cd SchemaLens_IoT_Cassandra_Stage4_Rev2
bash run_prebenchmark.sh
```

Verificar:

```bash
column -s, -t < outputs/stage8/30_hbench_summary.csv
python3 -m iot_pipeline.stage9 --dry-run
```

Os IDs Rev2 diferem dos IDs do Pilot 1 por incluir `experiment_revision` na assinatura. O Stage 8 contém SHA-256 de entradas, arquivos de regras e fontes da implementação, rejeitando alterações posteriores ao congelamento. O resultado prebenchmark é **somente do fixture A1**, não foi gerado com a sequência oficial `QUERY_SEED`.

O HBench do Pilot 1 foi arquivado (sem reclassificação nem edição) em `archive/pilot1_freeze_snapshot/`. O HBench Rev2 está em `outputs/stage8/`.

## 5. Executor Stage 9 preparado para sua máquina

**Não execute agora se quiser primeiro validar a nova seleção com uma workload independente.** Quando fizer o experimento local, reutilize Cassandra descartável da porta `9043`, após verificar que está saudável:

```bash
python3 -m iot_pipeline.stage9 --live --all \
  --confirm-reset-all RESET_ALL_SL_IOT \
  --host 127.0.0.1 --port 9043 \
  --warmup 5 --rounds 20 \
  --output outputs/stage9_rev2_run1
```

Os IDs novos produzem keyspaces novos `sl_iot_h_*`. O comando `--all` **apaga e recria** esses keyspaces experimentais. Use apenas Cassandra descartável. Se a instância do projeto anterior já está usando 9043, **não inicie outro Docker Compose na mesma porta**. Há suporte de teste com fake session, mas não houve teste contra servidor Cassandra para a nova TS2-U nesta sessão.

O comparador com baseline exige um manifesto da referência compatível com **este** novo HBench. Não misture latências do Pilot 1 com as de Rev2 nem apenas atualize hashes artificialmente.

## 6. Questões ainda abertas para fidelidade ao IoT Benchmark

1. O cliente Java oficial `GenerateQueryWorkLoad` utiliza parâmetros derivados da configuração/semente; nossa A1 escolhe instâncias ilustrativas, não faz replay da sequência oficial.
2. Para Q8, a implementação oficial da estratégia IoTDB `table` utiliza agregações do tipo `last`/`last_by`, não uma simples leitura da última linha de toda a série sem validar predicados.
3. Para Q12, operações de conjunto devem preservar a projeção, compatibilidade de tipos e semântica `UNION`, `INTERSECT`, `EXCEPT` do SQL efetivamente gerado. A1 implementa somente um exemplo de união e comparação simplificada de identidade.
4. Não há avaliação de ingestão concorrente, fatores de compactação, backfill, partições de longa duração, espaço em SSTable ou throughput multi-cliente.

**Referência de código upstream**: https://github.com/thulab/iot-benchmark — consultar `core/.../GenerateQueryWorkLoad.java`, `iotdb-2.0/.../ModelStrategy/TableStrategy.java` e fixar commit antes de alegar equivalência oficial.

### Resumo dos limites científicos

**Demonstrado offline:** seis candidatos físicos (TS0, TS1, TS2-D, TS2-U, TS4, TS5), Stages 4–8, cinco deployments HBench, 60 verificações de resposta, 35 testes unitários. **Não demonstrado:** desempenho de Rev2 em Cassandra, superioridade frente à baseline, correção para todos os parâmetros oficiais e escalabilidade.
