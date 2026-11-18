# SchemaLens — Baseline Cassandra nativa V3 (após o congelamento)

**Objetivo**: comparar os **quatro** HBench já congelados com uma baseline independente, estruturalmente inspirada no **IoTDB Table Model**, traduzida para Cassandra. **Não** se trata de um esquema Cassandra publicado pelo IoT Benchmark, nem de replay do benchmark SQL original.

## 0. Não use o projeto V3 principal como local de medição

Todas as operações de comparação devem ocorrer na **pasta isolada que foi validada no Ubuntu**:

```
native_auto_work/runs/v3_20261016T191603Z_pid2543627/
  schemalens_q12fixed_20261016T193745Z_pid2557748
```

Manifesto validado: hash de `28_hbench_frozen.json` =
`2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb`.

A seleção está congelada e continua com quatro designs:

- `H_2641c9751dce` — TS0+TS4
- `H_4f73edf0446d` — TS0+TS2-U+TS4
- `H_c7646ce7d864` — TS0+TS2-D+TS4
- `H_0bd185590c5a` — TS0

Nunca aplique `run_prebenchmark.sh` nem `iot_pipeline.run` de novo nessa pasta. O complemento somente **acrescenta** os arquivos `iot_pipeline/native_reference_v3.py`, `iot_pipeline/compare_native_v3.py`, `tests/test_native_reference_v3.py`.

## 1. Instalação e verificação em um único comando (OFFLINE)

Posicione `SchemaLens_Native_Reference_V3_Addon.zip` na pasta `SchemaLens_IoT_Cassandra_V3_Automatico`, execute estes **dois comandos**:

```bash
unzip -o SchemaLens_Native_Reference_V3_Addon.zip
bash SchemaLens_Native_Reference_V3_Addon/run_native_reference_v3.sh
```

O script identifica automaticamente a **única pasta Q12-fixed** cujo HBench possui o hash esperado. Se encontrar mais de uma execução compatível, recusa escolher arbitrariamente e pede o caminho explícito.

Caso seja necessário especificar manualmente:

```bash
EXP="native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748"
python3 SchemaLens_Native_Reference_V3_Addon/scripts/install_native_reference_v3.py \
  --experiment "$EXP" \
  --expected-hbench 2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb
```

A automação:

1. Verifica as entradas congeladas e seus hashes a partir de `32_frozen_manifest.json`;
2. Verifica o relatório `VALIDATION_RESULT.json` quando existir;
3. Copia **somente novos módulos** para a pasta congelada;
4. Executa 12 testes (Cassandra **simulado**, sem tocar no banco);
5. Verifica `Q1...Q12` contra o mesmo oráculo lógico usado no Stage9;
6. Localiza o `native_queries_q12fixed_*.jsonl` original, caso esteja na pasta pai, e compara seu hash com o registrado nas consultas;
7. Grava `outputs/native_reference_v3_precheck/NATIVE_REFERENCE_PRECHECK.json`;
8. Confere novamente que os hashes dos arquivos Stage4–8 permanecem inalterados.

Se terminar com `SUCCESS`, a **baseline está preparada**, mas ainda **não foi medida**.

## 2. O que representa a baseline

Para dois sensores presentes nos dados Java (`s_0`, `s_1`), a baseline usa:

```sql
CREATE TABLE iot_reference_table (
  device_id text,
  timestamp_ms bigint,
  m_s_0 double,
  m_s_1 double,
  PRIMARY KEY ((device_id), timestamp_ms)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC);
```

Uma linha por `(device_id,timestamp_ms)`, sem bucket temporal. É uma baseline **independente** e **externa ao catálogo de candidatos**. Não entra no Stage4, no Pareto nem no HBench.

A correspondência semântica interna usa a mesma implementação `_result` da V3:

- Q1: timestamp exato;
- Q2, Q3, Q4, Q6, Q7, Q9, Q10, Q11: intervalos `[start,end)` traduzidos do Java, filtros e agregações;
- Q5: sem restrição de tempo, mas com filtro de valor;
- Q8: último valor global disponível por sensor/dispositivo;
- Q12: cada filho preserva seus próprios dispositivos, sensores e intervalos; conjunto de tuplas projetadas com UNION, INTERSECT ou EXCEPT.

O comparador não interpreta os resultados dessa baseline como SQL oficialmente executado pelo IoTDB: todos são **resultados do contrato lógico traduzido e processado por Python**.

## 3. Se desejar executar fisicamente no Cassandra (Stage 9)

**Pré-condições:** Cassandra descartável, porta local `9043` mapeada para `9042` do container; biblioteca Python `cassandra-driver` instalada no `venv`; backup de qualquer experimento anterior relevante. Os comandos abaixo APAGAM keyspaces **experimentais específicos**, nunca um banco de produção. **Não execute ainda se quiser apenas validar offline.**

```bash
cd "$EXP"

python3 -m iot_pipeline.native_reference_v3 --live \
  --host 127.0.0.1 --port 9043 \
  --warmup 5 --rounds 20 \
  --confirm-reset RESET_SL_IOT_REF_NATIVE_V3 \
  --output outputs/stage9_native_reference_v3
```

Isso recria apenas o keyspace **`sl_iot_ref_native_v3`**, que é distinto de `sl_iot_ref_table` do Pilot 1. Primeiro carrega todas as linhas, depois compara os resultados das 12 consultas com o oráculo lógico. Mede tempos somente após a correção ter sido comprovada. Escreve:

- `34_load_metrics.csv`
- `35_query_p95.csv`
- `36_raw_latencies.csv`
- `37_manifest.json` com hashes do dataset, consultas e HBench.

**HBench físico**, com a mesma carga e repetição, no **mesmo experimento isolado**:

```bash
python3 -m iot_pipeline.stage9 --live --all \
  --host 127.0.0.1 --port 9043 \
  --warmup 5 --rounds 20 \
  --confirm-reset-all RESET_ALL_SL_IOT \
  --output outputs/stage9_hbench_native_v3
```

A etapa HBench acima recria os quatro keyspaces `sl_iot_h_<ID>`. Atenção: a **ordem fixa** desses deployments e a execução separada da baseline são potencialmente enviesadas por aquecimento, cache e ordem de teste. Esse ensaio só é diagnóstico, não um resultado de publicação.

## 4. Comparar após ambos os experimentos LIVE terminarem

```bash
python3 -m iot_pipeline.compare_native_v3 \
  --hbench outputs/stage9_hbench_native_v3 \
  --baseline outputs/stage9_native_reference_v3 \
  --output outputs/compare_native_v3
```

Compara p95 por consulta e a **média não ponderada dos 12 p95**, verificando todos os hashes disponíveis. Como o **Stage9 antigo não grava explicitamente o hash de query_instances no seu `37_manifest.json`**, isso fica sinalizado no `comparison_manifest.json`; não é possível afirmar verificação independente completa do trace passado.

## 5. Limitações que permanecem

1. O gerador Java exporta **um objeto de parâmetros por classe Q1–Q12**; isso **não** reproduz toda a sequência, frequência ou concorrência de consultas do cliente IoT Benchmark.
2. As consultas são avaliadas com o oráculo lógico `_result` da V3, com pós-processamento em Python. Elas **não** foram verificadas individualmente contra o SQL executado pelo cliente IoTDB; consultar esse banco é um experimento posterior.
3. O carregamento usa Python sequencial; `load_elapsed_seconds` **não** é o throughput oficial de ingestão IoT Benchmark.
4. O tamanho de dataset disponível é o piloto (1.800 células na execução validada); são necessárias maiores escalas e ordens balanceadas para publicação.
5. Este pacote não reabre a seleção congelada, não refaz o Stage8 e **não ajusta os custos analíticos a partir das latências**.

## 6. Se houver erro

Envie o bloco final do terminal após rodar o instalador. O comando offline executa toda a validação necessária sem Java, Maven ou Cassandra.
