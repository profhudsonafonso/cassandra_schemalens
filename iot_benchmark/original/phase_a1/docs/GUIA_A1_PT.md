# Etapa A1 — Integração do IoT Benchmark com SchemaLens / Cassandra

**Objetivo:** preparar o benchmark IIoT como uma segunda workload de séries temporais, preservando o catálogo TS0–TS8 e os modificadores M1–M5. Não empregar G0–G9 documentais nem C0–C9 Cassandra geral.

## 1. Distinção entre o benchmark e o SchemaLens

O IoT Benchmark fornece dados temporais e classes de operações. O SchemaLens decide quais alternativas físicas analisar **antes de qualquer medição**. Esta extensão usa Cassandra como banco-alvo, portanto precisamos de um executor próprio. O site oficial não lista Cassandra como DB_SWITCH. O projeto oficial é https://github.com/thulab/iot-benchmark .

## 2. O que o código-fonte do benchmark escreve

Arquivos analisados no repositório upstream:

- `core/.../extern/CSVSchemaWriter.java`: escreve `schema.txt` e `info.txt`. Em `schema.txt`, cada linha segue `device sensor sensorTypeOrdinal`. Exemplo `device_0 s0 4` significa `DOUBLE`, pois o ordinal de DOUBLE é 4 na enumeração atual.
- `core/.../extern/CSVDataWriter.java`: `FILE_PATH/<device>/BigBatch_<n>.csv`; a cada batch grava `Sensor,s0,s1,...` e linhas `timestamp,value0,value1,...`. Células ausentes saem vazias.
- `core/.../source/CSVDataReader.java`: leitura nativa. Nossa implementação Python usa o contrato do **escritor**, inclusive cabeçalhos entre batches. O leitor Java tem tratamento próprio que não devemos reproduzir cegamente.
- `core/.../workload/GenerateDataWorkLoad.java`: intervalos temporais dependem de `POINT_STEP`, timestamp precision, loop e configuração de ordem. Sementes e funções determinam os valores gerados.
- `core/.../workload/GenerateQueryWorkLoad.java`: tipos de consulta e parâmetros, incluindo `QUERY_SEED`, limites temporais, filtros e granularidade de grupos.

**Não simulamos o gerador Java:** o fixture é apenas um teste de compatibilidade do formato CSV. Para experimentos reais, os CSVs devem vir do gerador oficial, com versão/commit fixados.

## 3. Exemplo passo a passo

No arquivo `fixtures/native/schema.txt`:

```text
device_0 s0 4
device_0 s1 4
```

No `fixtures/native/device_0/BigBatch_0.csv`, as primeiras linhas são do tipo:

```csv
Sensor,s0,s1
1704067200000,0.0,1.0
1704067201000,5.0,3.0
1704067202000,10.0,5.0
```

O conversor transforma cada célula num registro lógico `(device_id, sensor_name, timestamp, value_double)`. Os dados são armazenados em `outputs/normalized_double_readings.csv`.

A leitura funciona em memória com um limite `--max-rows`, para evitar que um CSV de escala grande seja carregado inadvertidamente. Na Etapa A2, o pipeline físico utilizará streaming/batches.

## 4. Classes Q1–Q12 e o que é preservado

| Operação | Semântica documentada | Ativação inicial | Observação de implementação |
|---|---|---|---|
| Q1 | ponto exato | TS0, TS2 | acesso por tempo e dispositivo |
| Q2 | intervalo temporal | TS0, TS1, TS2 | múltiplos buckets para TS1/TS2 |
| Q3 | intervalo + filtro de valor | TS0, TS1, TS2 | filtragem não-chave no cliente, não ALLOW FILTERING |
| Q4 | agregação por intervalo | TS0 | redução raw na aplicação piloto |
| Q5 | agregação + filtro de valor sem janela estreita | TS0 | requer varredura completa no horizonte correto |
| Q6 | agregação + tempo + valor | TS0 | dois predicados |
| Q7 | GROUP BY tempo | TS0, TS5 | rollup exige janelas alinhadas e manutenção correta |
| Q8 | latest point com janela | TS0, TS4 | snapshot global pode ser insuficiente para janela histórica |
| Q9 | intervalo temporal DESC | TS0, TS1, TS2 | ordenação global após leitura de buckets |
| Q10 | intervalo com valor DESC | TS0, TS1, TS2 | filtro e ordenação no cliente |
| Q11 | GROUP BY temporal DESC | TS0, TS5 | alinhar janela, ordenar o resultado |
| Q12 | set operations (modelo table) | TS0, TS1, TS2 | combinar resultados de duas/múltiplas subconsultas no cliente; equivalência de projeção ainda pendente |

A Etapa A1 gera 26 vínculos consulta/família. A redução nominal ilustrativa é `1 - 26/(12*9) = 75,93%`. **Este indicador não é DSR validado de projetos executáveis** e nenhum Top-k, regret ou near-best foi medido.

## 5. Escopo e limitações das especificações CQL

`TS0`: tabela `(device_id,sensor_name)` + timestamp DESC, sem bucketing. É controle temporal raw, **não** replica o TSBS Cassandra oficial.

`TS1`: bucket UTC diário na PK. O QueryPlan deverá enumerar dias percorridos, inclusive bordas.

`TS2`: representação wide de todos os sensores presentes num timestamp do dispositivo. Células faltantes não podem ser inventadas.

`TS4`: snapshot materializado do último ponto. É inadequado para Q8 histórico sem caminho de fallback; manutenção concorrente ou out-of-order não está implementada.

`TS5`: SUM/COUNT/MIN/MAX por janela fixa em uma tabela. `AVG` requer `SUM/COUNT`; janelas relativas ao início da consulta só correspondem a rollups preexistentes se estiverem alinhadas. Correção incremental e substituição de pontos ainda precisam ser implementadas.

As outras famílias TS3/TS6/TS7/TS8 continuam no catálogo, mas não ativaram nesse cenário, portanto não recebem candidatos físicos artificiais.

Os DDLs foram validados **apenas estruturalmente** por testes Python; sua execução em Cassandra ainda é pendente.

## 6. Por que não rodar Stages 4–9 sem adaptar?

- Stage 4: função `build_prototypes` rejeita qualquer schema que não tenha exatamente 10 campos `usage_*`; gera tabelas `ts*_cpu*`.
- Stage 5: planos e condições específicas de consultas TSBS CPU-only.
- Stage 6: escrita inicial e maintenance assumem observações CPU-only e seu formato nativo TSBS Cassandra.
- Stage 7: proxies analíticos definidos para candidatos físicos TSBS.
- Stage 8: aceita `{TS0,TS1,TS2,TS5,TS6}` e exige artefatos/manifestos das fases anteriores com os mesmos IDs/hashes. TS4 não pode passar.
- Stage 9: leitor `load_cpu_dataset` exige prefixo `series_double,cpu` e 16 campos por linha. O IoT Benchmark grava diretórios por dispositivo.

Esses são **limites de contrato**, não defeitos do código anterior. Um adaptador 'iot' deve conservar a interface conceitual (CandidateSpec, QueryPlan, WritePlan, custo, HBench, execução) sem falsificar identidades físicas.

## 7. Protocolo de avaliação proposto

1. Fixar commit upstream, gerar dataset real (semente e escala), importar/verificar contagem e checksum.
2. Obter o texto exato das consultas executadas e sequência dos parâmetros. Não chamar as instâncias demonstrativas geradas em A1 de replay oficial.
3. Extrair evidência semântica das consultas; ativar TS-only usando o motor original. Congelar as decisões e regras, antes de medir.
4. Gerar CassandraCandidateSpecs TS-only sem assumir melhora de desempenho; acrescentar índices/chaves viáveis, WritePlans consistentes e regras de manutenção.
5. Validar QueryPlans por equivalência, especialmente Q5, Q8, Q12, filtros de valor, NULL, dados atrasados e agrupamentos não alinhados. Candidatos sem cobertura devem ser explicitamente rejeitados ou requerer fallback válido.
6. Estimar proxies **não medidos**, verificar compatibilidade física, construir configurações completas e selecionar HBench; congelar o manifesto.
7. Em Cassandra de teste: materializar cada deployment completo em keyspace isolado, carregar os mesmos dados, aguardar consistência, warmup, medir múltiplas repetições com mesma sequência de consultas. Armazenar p95 por Q, throughput de ingestão, bytes, erros, checksums e latências brutas.
8. Comparar com baseline externo Cassandra e com políticas de referência. Calcular preservação Top-1/Top-3 e near-best **apenas quando houver espaço de referência medido comparável** e após a seleção estar congelada.

## 8. O que já foi testado nesta A1

Execução `PYTHONPATH=. python3 -m unittest discover -s tests -v`: 16 testes. Incluem parser nativo, cabeçalho repetido, nulos, limite de volume, TS-only, não uso de latências na ativação, semântica demonstrativa de Q5/Q7/Q8/Q12 e bloqueio por falta de cobertura.

**Não há resultados experimentais de Cassandra nem de qualidade de HBench.**

## 9. Próximo marco, A2

Implementar um executor Cassandra mínimo sobre `TS0` e `TS1`, e um capturador de queries IoT Benchmark para obter os parâmetros reais. Somente depois adicionar `TS2`, `TS4`, `TS5` com WritePlan/QueryPlan e testes de correção, liberando assim as entradas para Stages 7, 8 e 9 adaptados.
