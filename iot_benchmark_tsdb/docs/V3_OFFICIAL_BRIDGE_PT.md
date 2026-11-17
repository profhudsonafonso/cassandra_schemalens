> **Atualização:** a etapa local A e B abaixo agora podem ser feitas automaticamente com `./run_v3_auto.sh`. Veja [o guia da automação](V3_AUTO_INTEGRACAO_PT.md). Os comandos manuais abaixo são mantidos como referência técnica.

# V3 — Ponte de parâmetros reais do IoT Benchmark para SchemaLens

## Objetivo e rigor

V3 é um **experimento novo e separado** da Rev2 e do Pilot Experiment 1. Não há medição de benchmark no pacote. Seu escopo é Cassandra e **exclusivamente** TS0–TS8 (modificadores M1–M5), sem ativar C0–C9. Não usa nenhuma latência anterior como entrada dos Stages 1–8. A família TS2 mantém variantes diária e sem bucket, com a exclusão mútua e limitação de partição da Rev2.

A ponte diferencia três níveis de evidência:

1. `generateDataMode`: **formato de dados oficial**, com `schema.txt`, `info.txt` e `device/BigBatch_*.csv`.
2. `SchemaLensQueryCapture`: **objetos de parâmetros reais** gerados pelo Java `GenerateQueryWorkLoad`, conforme `QUERY_SEED`, usando **uma amostra por classe Q1–Q12** nos Stages 4–8. Isso **não é** execução da ordem/frequência do scheduler, nem captura dos SQLs preparados por IoTDB.
3. **Ainda não obtido**: equivalência comprovada com os SQLs efetivamente executados e o tráfego misto gerado pelo benchmark; sem isso, não alegar reprodução oficial ou comparação publicável.

## Fonte verificada

Inspecionada a revisão de [thulab/iot-benchmark](https://github.com/thulab/iot-benchmark), commit `62e0dc100e56443978c8b4c66472b1d8a7ef0238` (2026-09-15), especialmente:

- `core/.../workload/GenerateQueryWorkLoad.java`: gera objetos das Q1–Q12, dispositivos e sensores escolhidos com `QUERY_SEED + clientId`, intervalo `QUERY_INTERVAL`, agregação e granularidade.
- `iotdb-2.0/.../ModelStrategy/TableStrategy.java`: criação de tabela, colunas TAG e FIELD, `date_bin`, filtro com `AND` para cada sensor, limite `time <= end`, Q8 `last()/last_by()` sem filtro temporal, e Q12 sobre projeções de sensores.
- `iotdb-2.0/.../IoTDB.java`: Q12 constrói um SQL composto de `childRangeQueries` e `setOpType`.

Esses fatos derivam do repositório oficial; o comportamento do executor Cassandra proposto é **uma implementação própria**, não validada como idêntica ao IoTDB nativo. A representação comparativa `iot_reference_table` também é uma **tradução**, não esquema Cassandra oficial.

## Correções na semântica normalizada V3

- **Q3, Q5, Q6, Q10:** para cada linha física (dispositivo, instante), **todos** os sensores selecionados devem satisfazer `sensor > threshold`. Antes, o piloto filtrava cada métrica separadamente.
- **Q7, Q11:** `date_bin(window,time)` com alinhamento ao *epoch*, sem ancoragem artificial no início da consulta.
- **Q8:** última leitura **global** no modo `table`, sem cláusula temporal em SQL. `TS4` é uma representação potencial; ela exige validação adicional de `last_by` em sensores NULL, atualizações e empates.
- **Q12:** operações `UNION`, `INTERSECT`, `EXCEPT` projetando apenas os **valores das colunas dos sensores** no modo normal, sem incluir `device_id` nem timestamp na identidade de linha. Os operandos nativos ficam no campo `set_children`.
- **Intervalos:** a fonte Java `TableStrategy.getTimeWhereClause` usa `[start,end]`, com limite superior **inclusivo**. Na importação convertemos para `[start,end+1)` (ms). O piloto A1 já usava intervalo semiaberto próprio; sua origem continua ilustrativa.

Ainda restam particularidades semânticas (NULL, tipos que não sejam DOUBLE, conversão de projeções para tabelas de saída, agregados vazios e limites da precisão). **Não usar os números do piloto como validação oficial.**

## Etapa local A — obter dados e objetos de consulta

Você precisa de **Java 17+ e Maven** para compilar o projeto Java oficial. O exportador V3 **não foi compilado contra o IoT Benchmark nesta sessão** (Maven não está instalado aqui); trate essa compilação como teste de integração no seu Ubuntu.

```bash
# Em uma pasta separada do SchemaLens
git clone https://github.com/thulab/iot-benchmark.git
cd iot-benchmark
git checkout 62e0dc100e56443978c8b4c66472b1d8a7ef0238

# Copie o arquivo java_bridge/cn/.../SchemaLensQueryCapture.java
# do pacote V3 para:
# core/src/main/java/cn/edu/tsinghua/iot/benchmark/tools/SchemaLensQueryCapture.java

# Configure configuration/conf/config.properties conforme seu experimento.
# Use IoTDB_DIALECT_MODE=table, TIMESTAMP_PRECISION=ms, NULL_RATIO=0,
# todos os sensores DOUBLE e um QUERY_SET_OP_NUM >= 2.
mvn clean package -Dmaven.test.skip=true

# Gerar dataset pelo modo generateDataMode (configurado no benchmark)
# ./iotdb-2.0/target/.../benchmark.sh (consulte o README do projeto original)

# Coletar uma amostra de parâmetro por classe, SEM consultar qualquer banco:
mvn -pl core exec:java \
  -Dexec.mainClass=cn.edu.tsinghua.iot.benchmark.tools.SchemaLensQueryCapture \
  -Dexec.args="/CAMINHO/ABSOLUTO/native_queries.jsonl 1"
```

O `ConfigDescriptor` procura a configuração em `configuration/conf/config.properties` por padrão, relativa ao diretório de execução do Maven. Opcionalmente, configure a propriedade JVM `benchmark-conf` se alterar a pasta de configurações. Capture os hashes da revisão Java, arquivo de configuração, dados e parâmetros em seu experimento.

O exportador itera as classes na ordem definida pelo enum Java, usando o gerador oficial para os atributos. Ele **não preserva a ordem de requisições, a frequência de operações nem a concorrência** do executor padrão. Não tente chamar o arquivo JSONL de log de queries executadas.

## Etapa local B — validar e importar a amostra

Na pasta V3, sem Cassandra:

```bash
python3 phase_a1/tools/official_query_capture.py \
  --capture /CAMINHO/ABSOLUTO/native_queries.jsonl --require-all-12 \
  --output outputs/official_capture_audit.json

bash scripts/run_native_prebenchmark.sh \
  /CAMINHO/DADOS_GERADOS \
  /CAMINHO/ABSOLUTO/native_queries.jsonl \
  /CAMINHO/ABSOLUTO/config.properties \
  500000
```

O `run_native_prebenchmark.sh` importa os dados, reativa apenas as famílias temporais por meio do motor original, reexecuta os Stages 4–8, congela o HBench e verifica suas consultas em memória. A suíte específica de baseline congelada do Pilot A1 é **excluída desse comando**, pois ela exige exatamente as 12 instâncias ilustrativas originais; o teste completo de 41 casos continua em `run_prebenchmark.sh` sobre o fixture incluído. Para não misturar experimentos, execute dentro **de uma cópia limpa do pacote V3**, sem reaproveitar artefatos do Pilot 1 ou Rev2. Quando a geração Java for realizada, envie `phase_a1/outputs/manifest.json`, `outputs/stage8/30_hbench_summary.csv` e `outputs/stage8/32_frozen_manifest.json` antes de qualquer Stage 9 com dados novos.

Os dados devem ter estrutura compatível com a ponte atual: `DOUBLE` em todos os sensores, timestamps em milissegundos, sem células nulas e nomes dos dispositivos idênticos aos serializados nos parâmetros Java. A ponte falha explicitamente quando detecta parâmetros não suportados. Para escalar, serão necessários loader em lotes, query-trace de múltiplas instâncias, pesos das frequências efetivas e suporte para sensores heterogêneos.

## Segurança e proveniência

- `run_prebenchmark.sh` opera no fixture ilustrativo fornecido, não no IoT Benchmark oficial.
- O adaptador nunca executa Cassandra sem `--live`, o Stage 9 preserva confirmação explícita de keyspace experimental, e o Stage 8 congela seleção por hashes.
- A versão V3 **não mede** SQL, p95 ou throughput, e **não se proclama reprodução do resultado oficial**.
- O `README` e o manifesto da seleção registram `official_iot_query_sequence_verified=false` até que uma reprodução fiel exista.
