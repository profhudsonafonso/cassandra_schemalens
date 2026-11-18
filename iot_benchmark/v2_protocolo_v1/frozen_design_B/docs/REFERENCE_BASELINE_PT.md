# Baseline de referência do IoT Benchmark — adendo pós-seleção

## Situação

A avaliação anterior **não** incluiu o esquema nativo do Apache IoTDB nem uma tradução independente dele no Cassandra. `TS0` é uma representação longa `((device_id, sensor_name), timestamp_ms)`; **não** é o esquema oficial do IoT Benchmark. A página oficial descreve os modos `tree` e `table`, enquanto a implementação Java `TableStrategy.registerTable` cria tabela com coluna `time` (TIME), `device_id` (TAG), sensores (FIELD), e demais tags quando configuradas.

Fonte: <https://iotdb.apache.org/UserGuide/latest/Tools-System/Benchmark.html>.
Código da implementação: <https://github.com/thulab/iot-benchmark/blob/master/iotdb-2.0/src/main/java/cn/edu/tsinghua/iot/benchmark/iotdb200/ModelStrategy/TableStrategy.java>.

**Não há esquema Cassandra oficialmente fornecido pelo benchmark.** Este adendo implementa uma **tradução Cassandra de referência do MODELO TABLE**; ela não substitui o benchmark nativo no Apache IoTDB nem a validação da semântica SQL oficial.

## Esquema de comparação independente

O equivalente estrutural para o fixture homogêneo DOUBLE/ms é:

```sql
CREATE TABLE IF NOT EXISTS iot_reference_table (
  device_id text,
  timestamp_ms bigint,
  m_s0 double,
  m_s1 double,
  PRIMARY KEY ((device_id), timestamp_ms)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC);
```

Uma linha por dispositivo + timestamp; métricas como colunas. **Não é TS0**, pois TS0 separa cada medida em uma linha; **não é TS2**, pois TS2 usa adicionalmente `bucket_day` na chave de partição. O esquema Cassandra traduz apenas a organização **lógica** básica da tabela IoTDB, não seus recursos específicos de armazenamento, índices ou compressão.

O benchmark de referência é executado **após** congelar `HBench` e **não é adicionado** ao Stage 4, 5, 6, 7 ou 8. Não utilizar resultados históricos ou esta baseline para alterar famílias ou pesos de pré-seleção.

## Instalação incremental no seu projeto atual

Na pasta existente `SchemaLens_IoT_Cassandra_Pipeline`, descompacte o adendo (sem sobrescrever seus outros scripts):

```bash
unzip SchemaLens_IoT_Reference_Baseline_Addon.zip -d .
source .venv/bin/activate
python3 -m unittest discover -s tests -p 'test_iot_reference_baseline.py' -v
```

## Etapa 1 — verificar offline (sem Cassandra)

```bash
python3 -m iot_pipeline.reference_baseline --dry-run
```

Esperado no fixture A1 original: 35 valores, 18 linhas físicas wide e 12 verificações de equivalência semântica **A1**, não do SQL oficial.

## Etapa 2 — executar no seu Cassandra local na porta 9043

AVISO: **apaga exclusivamente o keyspace experimental** `sl_iot_ref_table`. Não executar em Cassandra de produção nem em banco compartilhado.

```bash
python3 -m iot_pipeline.reference_baseline --live \
  --host 127.0.0.1 --port 9043 \
  --warmup 5 --rounds 20 \
  --confirm-reset RESET_SL_IOT_REF_TABLE \
  --output outputs/stage9_reference_table1
```

O executor verifica as mesmas Q1–Q12 do **fixture A1** antes de medir, gera os mesmos nomes de CSV do Stage 9 (`34_load_metrics.csv`, `35_query_p95.csv`, `36_raw_latencies.csv`, `37_manifest.json`), e utiliza a mesma definição nearest-rank do p95. As medições ainda estão sujeitas a cache/ordem e só são descritivas no piloto.

## Etapa 3 — comparar com os CSVs Stage 9 que você já mediu

```bash
python3 -m iot_pipeline.compare_baseline \
  --hbench outputs/stage9_run_all1 \
  --baseline outputs/stage9_reference_table1 \
  --output outputs/reference_comparison1

column -s, -t < outputs/reference_comparison1/reference_vs_hbench_summary.csv
```

Arquivos produzidos:

- `reference_vs_hbench_by_query.csv`: diferença de p95 por query, HBench/controle e variação percentual;
- `reference_vs_hbench_summary.csv`: média dos 12 p95 por deployment com pesos iguais, incluindo a baseline;
- `comparison_manifest.json`: ressalva metodológica e escala da comparação.

O comparador rejeita hashes do HBench/dados diferentes, resultados sem `correctness=PASS`, consultas/repetições inconsistentes ou deployments inesperados.

## Ressalvas para publicação

1. **Ainda não é a baseline IoTDB oficial medida:** o esquema está traduzido no Cassandra; medir o IoTDB nativo requer implantar IoTDB e executar seu cliente original.
2. **A1 não reproduz as queries Java:** os parâmetros e os operadores de Q8 e Q12 são simplificados; a baseline verifica apenas o contrato compartilhado do protótipo.
3. **Não comparar diretamente latências IoTDB e Cassandra como desempenho exclusivamente do esquema:** bancos e motores são diferentes.
4. **Ingestão sequencial do Python não equivale a throughput oficial IoT Benchmark**; usa 35 células lógicas e 18 linhas wide no fixture, com carga não concorrente.
5. **A baseline não foi executada aqui contra Cassandra:** rodar localmente; não existe valor oficial de p95 ainda.
6. **Não tratar os números como prova de superioridade:** obter maiores escalas, ordem intercalada/balanceada, múltiplas execuções e análise de variação/intervalos de confiança.

## Próximo experimento definitivo

Fixar uma versão/commit do gerador oficial, capturar `schema.txt`, `info.txt`, `BigBatch_*.csv`, a sequência das consultas reais e sua semântica SQL, executar A1/Stages 4–8 com esses insumos, congelar HBench, executar Cassandra controle e HBench, e (opcionalmente, em análise *cross-system* separada) executar o IoTDB nativo. No experimento de preservação de espaço, a baseline fica fora do conjunto controlado TS0–TS8 e da seleção HBench.
