# SchemaLens + TPCx-IoT 2.1.2 — Etapa 4 Cassandra

## 1. Objetivo e rastreabilidade científica

Implementar a **instanciação de candidatos físicos** (`γ_spec`) a partir das famílias **já ativadas nas Etapas 1–3**, seguindo a fronteira metodológica do artigo DKE *From Workloads to Explainable Document Schema Design Alternatives* (Seção 3.6). A implementação é para **Apache Cassandra**, como uma extensão de séries temporais, preservando as premissas dos artefatos TSBS prévios.

A Etapa 4 NÃO determina o melhor esquema, não executa o benchmark, não calcula latências e não valida cobertura de QueryPlan/WritePlan. A saída é um conjunto de **possibilidades físicas completas como definições de tabela**, ainda não deployments de workload.

O `inputs/stage3_frozen/` possui **exatamente** os cinco arquivos CSV/JSON enviados pelo usuário. O script valida identidade de queries, flags ativas, rastros das regras, modo de acesso, EER e metadados antes de gerar qualquer candidato. O manifesto `14_stage4_manifest.json` contém SHA-256 para auditoria.

## 2. Entradas analíticas congeladas

| Query | Root | Rc | D | Re(1) | A_TS(q) | Modificadores |
|---|---|---:|---:|---:|---|---|
| Q_INGEST | Reading | 0 | 0 | 0 | TS0 | M3 |
| Q_READ_2_WINDOWS | Reading | 1 | 1 | 0 | TS0, TS1, TS3 | M1, M3 |

O manifesto representa a leitura com `sensor_name` conhecido, mas **sem** `driver_system_id` vinculado. O predicado oficial do *kit* TPCx-IoT 2.1.2 ainda precisa ser validado. **Se a leitura exigir station+sensor, precisamos regenerar as etapas analíticas ANTES de medir ou selecionar candidatos**, pois TS3 pode deixar de ser ativado. A especificação textual descreve dois intervalos de 5s e horizonte histórico de 1800s, mas não é uma certificação de nosso SQL lógico ou adaptador Cassandra.

Outras pendências: publicação anterior descreve quatro variantes agregadas (MAX/MIN/AVG/COUNT). Nosso perfil de entrada atual é RAW. Não se devem confundir a operação de janelas do protocolo e a interpretação agregada sem inspecionar a implementação oficial.

## 3. Catálogo físico gerado

Uma família ativada pode produzir mais de uma variante física; a largura de bucket **não é uma família de ativação separada**.

| Família | Candidate ID | PK | CK | Variação |
|---|---|---|---|---|
| TS0 | `ts0_series_raw` | `(driver_system_id, sensor_name)` | `(event_ts, event_id)` | Série sem bucket; controle com partição potencialmente ilimitada |
| TS1 | `ts1_series_60s` | `(driver_system_id, sensor_name, bucket_start_ms)` | `(event_ts, event_id)` | Bucket 60s |
| TS1 | `ts1_series_3600s` | mesma da TS1 | mesma da TS1 | Bucket 3600s |
| TS3 | `ts3_sensor_60s` | `(sensor_name, bucket_start_ms)` | `(event_ts, driver_system_id, event_id)` | Bucket 60s, acesso por nome de sensor |
| TS3 | `ts3_sensor_3600s` | mesma da TS3 | mesma da TS3 | Bucket 3600s |

As PK e CK completas e o DDL constam de `outputs/tpcx_iot_stage4/cql/schemas/` e `10_cassandra_candidate_specs.json`.

### Colunas e fidelidade dos registros

Todos os cinco componentes originais estão preservados: `driver_system_id`, `sensor_name`, `event_ts`, `sensor_reading`, `padding`.

Adicionamos **somente à representação física experimental** o `event_id uuid` para preservar inserções com timestamps iguais e permitir idempotência por evento. O gerador do kit não é modificado aqui. O integrador da etapa 6 deverá produzir um ID **estável** para a mesma ocorrência durante reenvios; UUID aleatório a cada tentativa tornaria o replay não idempotente. `event_ts` é codificado como `bigint` em milissegundos desde Unix epoch UTC; essa convenção exige mapeamento consistente do timestamp da fonte.

`bucket_start_ms = floor(event_ts / (bucket_seconds * 1000)) * (bucket_seconds * 1000)`.

As larguras `{60, 3600}` são alternativas de política explícita da pesquisa, não valores extraídos da especificação TPC nem resultados otimizados.

### O que a estrutura TS3 resolve?

Uma leitura por `sensor_name` envolvendo várias subestações não consegue restringir diretamente uma partição de TS0/TS1 sem saber `driver_system_id`. TS0/TS1 exigem obtenção externa do conjunto de estações e execução dos acessos correspondentes. TS3 suporta a chave `sensor_name + bucket_start_ms`, permitindo varredura temporal sem conhecer o conjunto de estações. **TS3 duplica os dados**; qualquer deployment completo precisará garantir manutenção e contabilizar custo de escrita/armazenamento nas etapas seguintes.

## 4. Exemplo de leitura de duas janelas

Considere o sensor fictício `voltage` com janelas `TI1=[10:29:55,10:30:00]` e `TI2=[10:09:55,10:10:00]`, ilustrativas.

- **TS0**: consultar cada estação conhecida por sensor em cada janela. A consulta possui `driver_system_id = ? AND sensor_name = ? AND event_ts >= ? AND event_ts <= ?`.
- **TS1**: fazer o mesmo, segmentando cada janela por `bucket_start_ms`. Mesmo uma janela de 5 segundos pode cruzar o limite de dois buckets de 60 segundos.
- **TS3**: consultar o `sensor_name` e cada bucket envolvido, sem enumerar estações, recuperando `driver_system_id` nas linhas.

Os endpoints são **inclusivos** (`>=` e `<=`) para corresponder ao intervalo textual `[a,b]`; o plano da Etapa 5 definirá o tratamento correto de fronteiras, merges, duplicatas e eventual agregação.

> **Importante:** `12_query_read_primitives.json` são templates CQL parametrizados, não planos executáveis validados. Sem Stage 5 não afirmamos cobertura de consultas. `12b_write_primitives.json` são templates de escrita, não WritePlans de manutenção.

## 5. Modificadores

- M1: ativado pela consulta temporal com TS1; variantes de bucket ficam explícitas no arquivo de política.
- M3: sinaliza a necessidade de avaliar compaction apropriada a ingestão temporal; **não** escolhe ou habilita TWCS automaticamente.
- M2: TTL não é ativado (prazo de retenção desconhecido).
- M4: salting/sharding não é ativado (pressão de partições não observada).
- M5: política de backfill não é ativada (não há evidência de chegadas tardias).

Nenhuma estratégia de compactação, TTL, grau de replicação, nível de consistência ou tuning foi inferida silenciosamente.

## 6. Como reproduzir localmente (VSCode/Ubuntu)

Sem necessidade de Docker/Cassandra instalado para a Etapa 4:

```bash
unzip SchemaLens_TPCx_IoT_Cassandra_Stage4.zip
cd SchemaLens_TPCx_IoT_Cassandra_Stage4
./run_stage4.sh
# ou manualmente:
python3 -m methodology.tpcx_iot.stage4.cli
python3 -m unittest discover -s tests -v
```

Requisitos: Python 3.10+ (somente biblioteca padrão).

Saídas:

```
outputs/tpcx_iot_stage4/
  10_cassandra_candidate_specs.json
  11_query_candidate_map.json
  12_query_read_primitives.json
  12b_write_primitives.json
  13_query_candidate_map.csv
  14_stage4_manifest.json
  cql/schemas/*.cql
```

Para estudar DDL manualmente, abra cada `.cql` na pasta `cql/schemas`. A execução depende de criar um keyspace Cassandra adequadamente provisionado, que **não** é escolhido automaticamente pelo gerador.

## 7. Mapeamento para os algoritmos do artigo DKE

| Referência | Status |
|---|---|
| Alg. 1 / Seções 3.2–3.5: fragmentos EER, raiz, matriz, ativação | Artefatos congelados e verificados; não recalculados |
| Seção 3.6: `γ_spec` (candidate instantiation) | **Implementada** para as famílias ativadas TS0/TS1/TS3 |
| Alg. 4: deduplicação exata, fusão compatível, `ValidEquivalentPlan` | **Pendente**, não declaramos cobertura |
| Alg. 5: compatible covers, HBench, benchmarking | **Pendente** |

**Nota conceitual:** no Cassandra, *materialisation scope*, PK, CK, compaction e fanout de manutenção precisam ser representados de forma nativa; não copiamos literalmente as decisões de embedding do MongoDB do artigo DKE.

## 8. Próxima entrega: Etapa 5 (QueryPlans)

1. Definir o contrato exato de parâmetros das duas janelas e a interpretação correta da consulta no kit Java.
2. Para TS0/TS1, explicitar a origem da enumeração completa de estações, sem full scan nem `ALLOW FILTERING` invisíveis.
3. Para TS1/TS3, enumerar todos os buckets sobrepostos a cada janela, incluindo extremidades.
4. Validar resultados semânticos contra uma referência em memória com pelo menos dois `driver_system_id`, mesmo `sensor_name`, timestamps duplicados e intervalos que cruzam fronteiras de buckets.
5. Relacionar `origin(candidate)` e `cover(candidate)` distintamente; cobertura só após `ValidEquivalentPlan`.
6. Somente depois criar os WritePlans, os custos, as combinações completas HBench e as medições reais.

## 9. Limitações reais de execução

Os 20 testes executados são **offline**; não houve deploy Cassandra, carga oficial do kit, dois runs obrigatórios, mensuração IoTps, p95 ou análise Top-k. Não é resultado TPCx-IoT certificável. Os limites da TS0, as possíveis partições quentes na TS3 e a manutenção das réplicas permanecem questões de experimento.
