# SchemaLens Cassandra V4 Rev2 — Etapa 2: armazenamento físico

## Objetivo e justificativa

A auditoria de Stages 7 e 8 do experimento confirmatório registrou **24/24 MATCH** nas rotas instrumentadas, 12 rotas TS0 **NOT_INSTRUMENTED**, zero divergências, 49 hashes preservados e o HBench `R2H_e72ef4d8529d` (TS2-U), `R2H_bdc993acdde3` (TS2-D), `R2H_6355f4e57096` (TS0). Isso valida o modelo de contagens do *executor*, mas não valida o armazenamento físico.

Esta ferramenta coleta **dados físicos diretamente das tabelas Cassandra já materializadas**, sem fazer nenhuma escrita no banco. A coleta é separada do Stage 9 e não altera os resultados congelados.

**O que coleta:** tabelas e chaves físicas via `system_schema`; `SSTable count`, `Space used (live)`, `Space used (total)`, `Memtable data size`, `Number of partitions (estimate)` e compressão via `nodetool tablestats`, quando disponíveis. Associa cada keyspace ao HBench congelado e ao `load.csv` confirmatório. Os campos indisponíveis ficam como `N/D`; nunca são imputados como zero.

**O que NÃO mede:** bytes efetivamente transferidos para o cliente, p95 de latência do servidor, throughput de escrita concorrente, percentil real do tamanho de partições, número de leituras em disco por consulta. Também não executa `nodetool flush` ou `compact`.

## 1. Preparação

Execute dentro da raiz `SchemaLens_IoT_Cassandra_V3_Automatico`, com o venv ativo, e extraia `SchemaLens_Rev2_Stage2_PhysicalStorage.zip`:

```bash
unzip -o SchemaLens_Rev2_Stage2_PhysicalStorage.zip
COLLECTOR="$(realpath SchemaLens_Rev2_Stage2_PhysicalStorage)"
bash "$COLLECTOR/run_tests.sh"

WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"
NEW_EXP="$RUN/schemalens_rev2_ts5_contract_validated"
RESULTS="$NEW_EXP/outputs/v4_rev2_confirmatory/batch_15eadb0f"
OUT="$(realpath -m ../schemalens_rev2_physical_storage_20261105)"
```

O script **exige** os arquivos do congelamento original e recusa saída dentro de `NEW_EXP` ou `RESULTS`, além de recusar sobrescrever `OUT` já existente.

## 2. Descobrir onde `nodetool` está instalado

Se Cassandra roda via Docker e está exposto na porta local **9043**:

```bash
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}'
```

Anote o nome **real** do contêiner Cassandra e substitua `NOME_DO_CONTAINER` abaixo. Se o Cassandra roda diretamente no sistema e `nodetool` está no `PATH`, use apenas `nodetool`.

A primeira verificação é de leitura:

```bash
docker exec NOME_DO_CONTAINER nodetool tablestats sl_conf_r2_15eadb0f_reference
```

Se esse comando retorna estatísticas, o coletor poderá extraí-las. Caso o contêiner de confirmação não esteja mais disponível, **não** execute o benchmark novamente automaticamente: os keyspaces podem ter sido descartados. A auditoria de armazenamento requer essas tabelas preservadas. Registrar essa ausência é preferível a atribuir zeros.

## 3. Executar a coleta (somente leitura)

**Cassandra em Docker:**

```bash
python3 "$COLLECTOR/collect_storage.py" \
  --experiment "$NEW_EXP" \
  --results "$RESULTS" \
  --out "$OUT" \
  --host 127.0.0.1 --port 9043 \
  --nodetool-cmd "docker exec NOME_DO_CONTAINER nodetool"
```

**Cassandra local (nodetool no PATH):**

```bash
python3 "$COLLECTOR/collect_storage.py" \
  --experiment "$NEW_EXP" \
  --results "$RESULTS" \
  --out "$OUT" \
  --host 127.0.0.1 --port 9043 \
  --nodetool-cmd "nodetool"
```

**Somente esquema (coleta parcial, sem JMX):**

```bash
python3 "$COLLECTOR/collect_storage.py" \
  --experiment "$NEW_EXP" --results "$RESULTS" \
  --out "$OUT" --host 127.0.0.1 --port 9043 \
  --skip-nodetool
```

O último modo **não mede armazenamento**. Use `OUT` novo a cada execução (por exemplo, mude o sufixo de data/hora).

## 4. Saídas

```text
$OUT/
  manifest_storage.json
  RELATORIO_ARMAZENAMENTO_PT.md
  table_storage.csv
  deployment_storage.csv
  schema_<keyspace>.json
  nodetool_tablestats_<keyspace>.txt    (somente quando JMX/nodetool funciona)
```

**Interpretação dos campos:**

- `sstable_live_bytes`: estimativa de bytes de SSTables ativas na instância consultada, **não** tamanho lógico do dataset.
- `sstable_total_bytes`: SSTables incluindo arquivos que ainda podem existir por ciclos de compaction; não equivale a `sstable_live_bytes`.
- `memtable_data_bytes`: memória usada pelo buffer de escrita. Não constitui, por si só, armazenamento persistido no disco.
- `partition_count_estimate`: aproximação obtida do `nodetool`, não um percentil nem o tamanho máximo de partição.
- `physical_write_operations`: contagem já registrada no benchmark anterior (não é throughput).

A baseline original e a baseline Q8 otimizada **utilizam o mesmo keyspace e a mesma tabela física**. Seus tamanhos são uma única observação, não duas cópias que possam ser somadas.

## 5. Se `SSTable count = 0`

Isso pode acontecer porque as 9 mil linhas largas ou as 18 mil células TS0 ainda estão no memtable. O coletor **não executa** `nodetool flush`, para preservar o estado atual do sistema e evitar confundir uma coleta pós-flush com os tempos já medidos antes dele.

Para um futuro segundo snapshot *pós-flush*, escolha primeiro uma instância **descartável** ou preserve integralmente os resultados confirmatórios, registre a ação, execute `nodetool flush` somente nos keyspaces `sl_conf_r2_15eadb0f_*` do experimento e rode uma nova coleta com `OUT` distinto. Não compare as latências antigas com o novo estado físico como se tivessem sido medidas simultaneamente. Evite `compact` sem um protocolo específico.

## 6. Como avaliar os proxies posteriormente

O Stage 7 congelado oferece `storage_proxy` e `max_partition_bytes_proxy`. Compare com `sstable_live_bytes` e os dados reais de partição, **mas não espere igualdade numérica**: o proxy representa bytes estimados por modelo simplificado, enquanto SSTables possuem compressão, metadados, índices e estado de flush/compaction.

Para comparação válida use, na ordem: (1) ranking TS2-U vs. TS2-D vs. TS0 em bytes reais; (2) razões relativas sobre a baseline; (3) concordância de ordenação *rank agreement*; (4) magnitude do erro, apenas após explicitar unidades; e (5) tamanhos de partição usando instrumentação apropriada. A coleta não fornece bytes transferidos na rede: isso exige outra instrumentação.

A política Rev2 é imutável para esta campanha. Nenhum coeficiente pode ser alterado com base neste armazenamento e depois ser apresentado como validação independente da mesma campanha.

## 7. Compartilhar os dados para a próxima análise

```bash
cd "$(dirname "$OUT")"
zip -r stage2_storage_rev2_results.zip "$(basename "$OUT")"
```

Envie `stage2_storage_rev2_results.zip`. Isso permitirá auditar se a ordem de grandeza do `storage_proxy` está consistente com SSTables reais e decidir se é necessário medir tamanho de partições com `nodetool tablehistograms`/`sstablemetadata` em um protocolo futuro.

**Idioma:** código e comentários em inglês; explicações em português com termos técnicos em inglês.
