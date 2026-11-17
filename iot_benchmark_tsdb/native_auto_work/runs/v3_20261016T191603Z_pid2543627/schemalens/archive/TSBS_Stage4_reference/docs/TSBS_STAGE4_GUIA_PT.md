# Etapa 4: geracao de candidatos Cassandra para as familias TS do TSBS

## Objetivo

Continuar **somente** a etapa TSDB do SchemaLens, sem alterar o motor declarativo nem as familias C0-C9. As regras do SmartRetail V2 ja decidiram `A_TS(Qi)`; esta etapa **instancia** alternativas com PK/CK e esquemas CQL. E uma fase anterior a `QueryPlan`, `WritePlan`, cobertura, consolidacao, HBench e benchmark.

## Como as entradas se transformam em schemas

```text
outputs/tsbs_run/04_tsdb_activation_matrix.csv  (TS0-TS8)
outputs/tsbs_run/05_temporal_modifier_matrix.csv (M1-M5)
inputs/workload/query_manifest.csv               (kind de cada consulta)
inputs/schema/tsbs_cpu_only_schema.json          (metricas e identidade)
config/tsbs_stage4_physical_policy.json          (variantes fisicas predefinidas)
      ↓
methodology/tsbs/stage4/candidate_generator.py
      ↓
outputs/tsbs_stage4/{10..14}_*.json/csv + cql/schemas/*.cql
```

A regra de ativacao nao mudou. A coluna `kind` do manifesto so escolhe **qual forma fisica de TS5** instanciar, depois de TS5 estar ativa. QIDs sao usados apenas para cruzar linhas de arquivos, nunca para decidir a ativacao.

## Que variantes esta etapa emite?

| Familia ativada | Candidato | PK (partition key) | CK (clustering key) | Aplicacao |
|---|---|---|---|---|
| TS0 | ts0_metric_raw | (hostname, metric_name) | timestamp_ns DESC | Serie bruta sem bucket; controle de referencia (particao pode crescer demais) |
| TS1 | ts1_metric_bucket_1d | (hostname, metric_name, bucket_day) | timestamp_ns ASC | Historico raw particionado por 1 dia UTC |
| TS1 | ts1_metric_bucket_7d | (hostname, metric_name, bucket_day) | timestamp_ns ASC | Variante hipotetica de 7 dias UTC |
| TS2 | ts2_wide_bucket_1d | (hostname, bucket_day) | timestamp_ns ASC | Dez metricas na mesma linha temporal |
| TS5 | ts5_max_by_host_minute | (hostname, metric_name, bucket_day) | minute_ns ASC | MAX de uma/varias metricas por host e minuto |
| TS5 | ts5_max_by_host_hour | (hostname, metric_name, bucket_day) | hour_ns ASC | MAX por host e hora |
| TS5 | ts5_sum_count_by_host_hour | (hostname, metric_name, bucket_day) | hour_ns ASC | AVG por host e hora via SUM/COUNT |
| TS5 | ts5_global_max_minute | (metric_name, bucket_day) | minute_ns DESC | MAX global por minuto para ORDER BY/LIMIT |
| TS6 | ts6_hour_host_dimension | (bucket_day, hour_ns) | hostname ASC | Media agrupada por hora e host |

**Importante:** os nomes TS0-TS8 sao a taxonomia da pesquisa, nao tabelas preexistentes do TSBS. O TSBS Cassandra original tem tabelas por tipo (`series_double` etc.), com `series_id` que inclui tags, campo e data. Isso e um baseline externo que devemos rodar separadamente, nao um candidato duplicado artificiosamente aqui.

## Exemplo concreto Q13: high-cpu-1

Entrada de ativacao: `TS0 + TS1 + TS2`. A Etapa 4 gera a possibilidade de consultar 10 metricas separadas, de usar buckets raw de 1 ou 7 dias e de co-localizar as 10 metricas na tabela wide. A TS2 permite um `SELECT ... WHERE hostname=? AND bucket_day=? AND timestamp_ns>=? ...`, mas a condicao `usage_user > 90` deve ser aplicada no cliente neste candidato, porque nao estamos emitindo `ALLOW FILTERING`. Esta nao e uma promessa de eficiencia de threshold.

## Exemplo concreto Q01: single-groupby-1-1-1

Entrada: `TS0 + TS5`. A TS5 instancia `ts5_max_by_host_minute`, com `max_value` guardado por minuto e por metrica. Isso somente sera correto apos uma etapa de manutencao que precompute corretamente o MAX. Se uma consulta tiver inicio ou fim desalinhado com o bucket de rollup, um rollup integral pode produzir um resultado errado: deve haver fallback/compensacao raw, definida no QueryPlan (Etapa 5).

## Exemplo concreto Q10: double-groupby-1

Entrada: `TS0 + TS5 + TS6`. A TS5 gera uma alternativa `SUM(value)`/`COUNT(value)` por host/metrica/hora. A TS6 gera tabela wide de soma+contagem por hora+host, permitindo leitura de todos os hosts em cada hora. A media nao deve ser calculada como media de medias: deve ser `SUM(soma)/SUM(contagem)` nas janelas apropriadas.

## Modificadores

- M1 define que granularidade de bucket precisa ser explorada. As variantes `[1, 7]` dias foram **declaradas em politica de demonstracao antes de benchmark**, nao escolhidas como melhores.
- M2 TTL: nao adicionado sem horizonte de retencao explicitamente declarado.
- M3 compaction: ativo nos dados atuais, mas nao escolhemos automaticamente TWCS/STCS. A selecao e compativel com retencao, late data e versao do Cassandra; permanece pendente.
- M4 salt/shard: nao e materializado se inativo; a TS6 pode precisar de uma politica de distribuicao em escalas maiores. O gerador nao inventa sharding automatico.
- M5 late arrivals: nao muda semantics sem entrada explicitamente fornecida; planos de manutencao precisam definir tratamento antes dos benchmarks.

## Salvaguardas e limites

1. Nao usa benchmark antigo/externo para selecionar candidatos nem retroalimentar ativacoes.
2. Nao gera schemas para TS3, TS4, TS7 ou TS8: nao foram ativadas pela workload padrao. Se alguma se tornar ativa, o programa falha explicitamente ate que sua instanciação seja implementada.
3. A saida agrega candidatos compartilhados: uma tabela pode estar ligada a varias consultas, sem gerar um clone por QID.
4. Q15 (`lastpoint`) permanece TS0. A query exige ultima medicao **por host**, nao `LIMIT 1` global; TS4 nao foi ativada e nao ha atalho embutido.
5. Q14 consulta todos os hosts: sem chave `hostname`, TS0/TS1 exigem enumeracao de hosts, a ser especificada na Etapa 5.
6. Q16 requer os ultimos 5 agregados globais por minuto. TS5 gera tabela possivel, mas sem plano de manutencao o resultado nao esta garantido.
7. Nao ha `ALLOW FILTERING`; o codigo documenta filtragem residual no cliente.
8. Os CQL usam `?` como marcadores de consulta preparada, mas nao sao rotinas completas de execução. Tabelas agregadas NAO contêm dados ate a criacao dos WritePlans.
9. TS5 e TS6 nao sao automaticamente capazes de responder intervalos temporais desalinhados; preservar correcao requer fallback raw ou logica de borda.
10. Esta etapa gera esquemas isolados, ainda NAO uma configuracao completa e compativel para toda a workload.

## Execucao

```bash
./run_tsbs_stage4.sh
python3 -m unittest discover -s tests -v
```

Para mudar a granularidade raw, edite exclusivamente `config/tsbs_stage4_physical_policy.json`, registre a nova politica e regenere os candidatos **antes dos benchmarks**. O arquivo `14_stage4_manifest.json` salva o hash de cada entrada para auditoria.
