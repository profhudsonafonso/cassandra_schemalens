# Etapa 6 — WritePlans do SchemaLens para TPCx-IoT no Cassandra

## 1. Objetivo científico

Implementar a fase de **especificação de manutenção física** que complementa os
QueryPlans da Etapa 5. No artigo SchemaLens/DKE, a instância física contém
coleções/tabelas, referências, índices e **restrições de manutenção**; o projeto
completo registra tanto `Route_h` quanto `Maint_h`. A Etapa 6 prepara `Maint_h`
sem escolher um `HBench`, sem medir latências e sem refazer a ativação.

A carga é um **perfil acadêmico adaptado**, descrito pelos artefatos congelados.
A hipótese `WHERE sensor_name = ...` e o significado exato da operação analítica
ainda requerem verificação no código do kit oficial TPCx-IoT 2.1.2.
Não afirmamos que este pacote executa ou certifica o benchmark oficial.

## 2. Entradas congeladas

- `outputs/tpcx_iot_stage1_3/`: EER, matriz analítica, ativação `TS0, TS1, TS3`.
- `outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json`: cinco esquemas.
- `outputs/tpcx_iot_stage5/20_queryplans.json`: cinco planos de leitura.
- `config/tpcx_stage6_write_policy.json`: política explícita da escrita.

O gerador verifica integridade, versões, hashes e correspondência dos planos.
Não altera as entradas anteriores nem utiliza medições para selecionar famílias.

## 3. Os cinco WritePlans

| Candidato | Representação | Quantas gravações por evento quando selecionado? |
|---|---|---:|
| `ts0_series_raw` | por subestação e sensor, tempo como clustering | 1 |
| `ts1_series_60s` | por subestação, sensor e bucket de 60 s | 1 |
| `ts1_series_3600s` | por subestação, sensor e bucket de 3600 s | 1 |
| `ts3_sensor_60s` | projeção por sensor e bucket de 60 s | 1 |
| `ts3_sensor_3600s` | projeção por sensor e bucket de 3600 s | 1 |

Em um **experimento de correção offline** que mantém todas as cinco tabelas ao
mesmo tempo, cada evento causa cinco instruções. Isso **não é** o custo presumido
do desenho selecionado pelo SchemaLens. A Etapa 8 decidirá quais tabelas podem
coexistir em configurações completas e quais são redundantes.

### 3.1. Exemplo do bucket UTC

Para `event_ts=60_000` (milissegundos UTC desde a época Unix),
`bucket_start_ms=(60_000 // 60_000)*60_000 = 60_000` na TS1 60s.
Na TS1 3600s, `bucket_start_ms=0`.
Para instantes anteriores à época, a divisão é sempre **floor**:
`event_ts=-1` pertence ao bucket `-60_000`, não ao bucket `0`.

### 3.2. Exemplo do INSERT

```sql
INSERT INTO ts3_sensor_60s
(driver_system_id, sensor_name, event_ts, event_id,
 sensor_reading, padding, bucket_start_ms)
VALUES (?, ?, ?, ?, ?, ?, ?);
```

Os parâmetros são vinculados **na ordem das colunas da Etapa 4**.
O objeto `event_id` é um `uuid.UUID` no gerador Python; exemplos JSON o mostram
como string por razões de serialização. Para execução Cassandra, o driver deve
receber o tipo UUID, não uma string arbitrária. `padding` não é descartado.

## 4. Identidade, duplicatas e ordem de escrita

Uma entrada possui `driver_system_id`, `sensor_name`, `event_ts`,
`sensor_reading` e `padding` e **deve** fornecer pelo menos:

- `event_id`: UUID global e estável do registro **ou**
- `source_record_id`: identificador de origem *único e estável entre replays*.

Com `source_record_id`, o adaptador gera UUIDv5 determinístico a partir dos
quatro elementos `(subestação, sensor, timestamp, origem)`.
Não se deve usar timestamp como único identificador: dois registros podem
compartilhar o mesmo instante. O kit TPCx-IoT não foi comprovado como fornecedor
desse campo; a integração com a origem deverá defini-lo e validar sua unicidade.

**Semântica implementada:** append/INSERT de observações imutáveis.
Repetir um evento **idêntico** não gera novos comandos no modelo de referência;
reutilizar a mesma identidade para alterar leitura ou padding é rejeitado.
Deletes, correções históricas e TTL não são tratados nesta etapa e exigem
outra política explícita. A identidade estável permite reexecutar a mesma
instrução CQL idempotente.

## 5. Catálogo de subestações e TS3

TS0 e TS1 precisam de uma enumeração **completa** das subestações para atender a
leitura por nome do sensor. A classe `AuthoritativeRoster` exige lista sem
duplicatas, `asserted_complete=True` e uma identificação da fonte.
Essa declaração é fornecida pelo **chamador**; o módulo não consegue comprovar
que a lista está completa no mundo real nem persiste esse catálogo.

TS3 escreve a mesma observação na projeção por nome de sensor; as leituras
atravessam subestações sem enumeração. O WritePlan registra a dependência de
uma origem canônica recuperável para detectar e reconstruir projeções perdidas.

## 6. Falhas parciais e replay

O protótipo `ReferenceMaterializer` valida todo o lote em memória, antes de
atualizar seu estado de referência. O `InMemoryMutationSink` simula gravações
físicas e uma falha no meio do lote:

1. Gerar cinco mutações físicas por evento **somente** no teste com cinco tabelas.
2. Interromper após três mutações.
3. Detectar que o estado físico simulado divergiu do estado de referência.
4. Reexecutar as mutações do ledger **em memória**.
5. Verificar que a projeção voltou a ser consistente.

**Atenção:** isso não implementa journal durável, checkpoint, entrega
exactly-once, atomicidade transacional entre partições ou cluster Cassandra.
O Cassandra não oferece uma transação ACID genérica entre essas tabelas;
será necessário especificar um mecanismo durável de recuperação antes de
declarar equivalência operacional sob concorrência.

## 7. Como executar no VS Code

No terminal aberto na raiz do projeto:

```bash
python3 -m methodology.tpcx_iot.stage6.cli
python3 -m examples.show_stage6_writeplans
python3 -m unittest discover -s tests -v
# Alternativa:
./run_stage6.sh
```

Somente Python 3.10+ e bibliotecas padrão são necessários. Não há conexão
Cassandra nem dependência de driver Python nesta etapa.

## 8. Outputs

- `30_writeplans.json`: cinco planos físicos de inserção.
- `31_write_coverage.json`: cobertura **condicional** por operação de escrita.
- `32_offline_write_probes.json`: resultados de verificações offline.
- `33_writeplan_manifest.json`: rastreabilidade e SHA-256 das entradas.
- `34_example_mutations.json`: amostra serializada das mutações CQL.

Os artefatos não marcam nenhuma família como vencedora, nem selecionam HBench.

## 9. Relação com o artigo DKE

- **Seções 3.4–3.5:** evidências e ativação, já congeladas nas etapas 1–3.
- **Seção 3.6:** instanciar as especificações com `Consp` (manutenção física).
- **Seção 3.7/Algoritmo 4:** futura consolidação física e validação de cobertura.
- **Seção 3.7/Algoritmo 5:** futura seleção Pareto e benchmarks completos.

A Etapa 6 verifica apenas se planos de manutenção definidos por candidato têm
comportamento coerente em testes sintéticos, **não** sua compatibilidade ou
viabilidade distribuída quando combinados.

## 10. Pendências para Etapas 7–9

- Confirmar, no driver Java oficial, o predicado do sensor e as operações
  efetivamente executadas pelo kit TPCx-IoT.
- Derivar limites seguros de tamanho da partição TS0 e de volume por bucket.
- Definir custos analíticos de escrita/projeção/partições, sem usar resultados
  históricos de benchmark para escolher as famílias.
- Separar o ledger canônico e o mecanismo de replay em armazenamento persistente.
- Validar CQL, driver, consistência, falhas, quorum, concorrência e ingestão
  real em Cassandra nas configurações de workload selecionadas.
- Respeitar as condições e os limites da licença oficial do TPCx-IoT ao comparar.
