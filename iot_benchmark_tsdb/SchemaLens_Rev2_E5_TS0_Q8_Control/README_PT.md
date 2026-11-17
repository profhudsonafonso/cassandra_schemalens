# SchemaLens Cassandra — E5: controle da Q8 TS0 (`FULL_SCAN` vs. `DESC_LIMIT1`)

## 1. Objetivo científico

Este **experimento exploratório pós-hoc** investiga uma inconsistência detectada pela auditoria dos Stages 7/8 da V4 Rev2. Na avaliação confirmatória com a fonte canônica TS0:

- O **Stage 7 Rev2**, previamente congelado, previu **2 chamadas de `Session.execute` e 2 linhas físicas** para Q8.
- O **Stage 9 original** realmente executou **2 chamadas** e percorreu **3.000 linhas** para as duas séries solicitadas.
- A consulta Q8 precisa apenas do **último ponto observado por `(device_id, sensor_name)`**.

**Hipótese E5:** a diferença entre as 3.000 linhas da TS0 e as 2 linhas previstas é causada por um plano de consulta ineficiente (`FULL_SCAN`), não por uma limitação intrínseca da representação TS0.

**Atenção:** este E5 **não corrige retroativamente** o Stage 7/8, não seleciona uma nova configuração, não recalcula o HBench Rev2 e não substitui os valores p95 da campanha confirmatória. O resultado será usado para fundamentar uma futura Rev3, avaliada com novos dados.

## 2. O que será comparado

Ambos usam **a mesma tabela física já existente**, no keyspace Rev2 TS0:

```sql
-- A: the exact old Stage9 TS0 Q8 read (no time restriction):
SELECT timestamp_ms,value_double FROM ts0_iot_raw
WHERE device_id = ? AND sensor_name = ?;

-- B: the alternative plan, same partition and values:
SELECT timestamp_ms,value_double FROM ts0_iot_raw
WHERE device_id = ? AND sensor_name = ? LIMIT 1;
```

A TS0 tem `PRIMARY KEY ((device_id,sensor_name),timestamp_ms)` e `CLUSTERING ORDER BY (timestamp_ms DESC)`. As leituras são executadas por partição de sensor. O programa usa **o executor Stage 9 V3 real e inalterado no plano A**. O plano B é implementado isoladamente no add-on E5. Ambos produzem resultados pela mesma função lógica `_result`, e o programa exige equivalência com o **oráculo da captura Java confirmatória**.

**Por que `LIMIT 1` é válido aqui?** A carga materializada da TS0 deriva de células DOUBLE efetivamente observadas: sensores com valores ausentes não produzem linhas TS0; portanto, a primeira linha de cada partição temporal descendente já é a observação mais recente daquela série. Isso **não** garante segurança universal em outras semânticas de escrita. O plano B inclui um fallback conservador para uma linha inesperada com `value_double=NULL`, mas a **campanha medida aborta** caso esse fallback seja necessário, pois não seria uma comparação homogênea com o Stage 9 original. Os testes sintéticos cobrem células/esparsidade, sensores ausentes, último timestamp distinto, duplicação resolvida pelo Cassandra e valores nulos inesperados.

## 3. Proteções e limites

- **Somente `SELECT`.** Os dois únicos CQL de dados autorizados são as strings SQL constantes mostradas acima. O programa também usa um `SELECT` de metadados `system_schema.tables` para confirmar que a tabela existe.
- **Sem `INSERT`, `DROP`, `CREATE`, `UPDATE`, `DELETE`, `flush` ou `compact`.** O banco e a seleção Rev2 não são modificados.
- Valida antes de conectar os hashes do HBench congelado, as fontes do benchmark e os arquivos já existentes; verifica novamente depois.
- Não cria tabelas, não gera outra carga, não aceita outro keyspace, nem outros parâmetros de consulta Q8.
- Falha caso um resultado não corresponda ao oráculo ou caso apareçam dados nulos que forcem fallback na campanha medida.
- **Contagem de chamadas = `Session.execute` Python; não é o total de viagens de rede.** **Linhas iteradas = registros consumidos pelo cliente; não equivale a linhas/tombstones lidos no armazenamento interno.** A latência inclui driver + desserialização + processamento Python.
- `--pairs 200` produz **200 medições por plano**, cada par executando os dois planos, em blocos AB ou BA distribuídos igualmente e embaralhados com seed fixa. Pareamento reduz certos efeitos de deriva, mas **não transforma uma única execução em 200 campanhas independentes**.
- Os resultados são experimentais. Não afirmaremos ganho até executar o script no Cassandra e auditar os CSVs.

## 4. Instalação e testes offline

Na pasta principal `SchemaLens_IoT_Cassandra_V3_Automatico`:

```bash
unzip -o SchemaLens_Rev2_E5_TS0_Q8_Control.zip

E5="$(realpath SchemaLens_Rev2_E5_TS0_Q8_Control)"
TELEM="$(realpath SchemaLens_Rev2_TS0_Telemetry)"
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"

bash "$E5/run_tests.sh"
```

**Esperado:** `Ran 14 tests ... OK`. Todos os testes são simulados, sem conexão Cassandra. A execução anterior de integração local também confirmou que o `Stage9.execute_query` original e o novo plano devolvem resultados idênticos sobre uma sessão falsa, com 4 vs. 2 linhas de exemplo.

O pacote requer a presença das pastas dos add-ons `SchemaLens_Rev2_TS0_Telemetry` e `SchemaLens_V4_Rev2_Confirmatory`, já usadas na etapa anterior.

## 5. Executar o controle E5 em Cassandra

Após os testes e verificando que a instância Cassandra de pesquisa ainda contém os keyspaces Rev2:

```bash
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"
NEW_EXP="$RUN/schemalens_rev2_ts5_contract_validated"
RESULTS="$NEW_EXP/outputs/v4_rev2_confirmatory/batch_15eadb0f"

# A fresh output directory; never remove or overwrite earlier evidence.
OUT="$(realpath -m ../schemalens_rev2_e5_q8_$(date +%Y%m%d_%H%M%S))"

bash "$E5/run_e5.sh" "$NEW_EXP" \
  --results "$RESULTS" \
  --addon "$ADDON" \
  --out "$OUT" \
  --host 127.0.0.1 \
  --port 9043 \
  --pairs 200 \
  --warmup 10 \
  --seed 20261105 \
  --fetch-size 128
```

**O programa não materializa dados**, portanto não use keyspaces antigos ou outros ambientes de Cassandra. Se a conexão falhar porque as tabelas foram removidas, o programa interromperá a execução sem recriar estruturas.

**Se a execução falhar com `Refusing to overwrite existing E5 output`**, crie um novo `OUT` com outro timestamp; não delete a coleta antiga.

## 6. Arquivos gerados

```text
$OUT/
  e5_raw.csv          # Each measured execution: latency, plan, calls, iterated rows, oracle
  e5_paired.csv       # One AB/BA pair per row; difference = LIMIT1 - FULL
  e5_summary.csv      # mean, median, p95, p99, counts per plan
  manifest.json       # frozen hashes, source, seed, preflight, limitations
  RELATORIO_E5_PT.md  # Portuguese interpretation generated by the script
```

Para compartilhar os resultados:

```bash
cd "$(dirname "$OUT")"
zip -r stage4_e5_ts0_q8_results.zip "$(basename "$OUT")"
```

Envie o arquivo `stage4_e5_ts0_q8_results.zip` para auditoria.

## 7. Como interpretar

Exemplo **hipotético** de contagem estrutural, **não uma medição de latência nova**:

| Métrica | `FULL_SCAN` (A) | `DESC_LIMIT1` (B) |
|---|---:|---:|
| Chamadas de `Session.execute` | 2 | 2 |
| Linhas iteradas | 3.000 | 2 |
| Resultado de Q8 | Duas últimas observações | As mesmas duas observações |

Somente depois de rodar no Cassandra compararemos média, mediana, p95 e diferença pareada. Um valor `B - A < 0` significa menor latência para o plano otimizado. A avaliação de p95 com 200 repetições é mais estável que com 40, mas ainda exige mais campanhas independentes para inferência robusta.

Caso `LIMIT1` melhore significativamente, a conclusão é **planejamento físico de Q8 inadequado no Stage 9 Rev2**, não que TS0 necessariamente seja a melhor arquitetura para o workload inteiro. Quando TS0 também usa um plano eficiente, sua diferença em relação às demais famílias deverá ser reavaliada **somente em uma nova Rev3 com HBench e holdout novos**.

## 8. Passo seguinte (Rev3 e crescimento)

Depois da auditoria E5, o planejamento metodológico é:

1. **Stage 5 Rev3:** produzir um `query_plan_id` ou `executor_plan` explícito por par `(query, candidate)`, como `TS0_Q8_DESC_LIMIT1`, `TS0_Q8_FULL_SCAN`, `TS2U_Q8_LAST_NON_NULL` e `TS2D_MULTI_BUCKET_SCAN`; rejeitar planos fisicamente não implementados.
2. **Stage 7 Rev3:** computar `request_count_proxy`, `physical_rows_proxy`, `bucket_fanout_proxy`, bytes estimados e tamanho de partição a partir do plano realmente executável; se o plano for `FULL_SCAN`, estimar o histórico inteiro, não apenas a cardinalidade da resposta.
3. **Stage 8 Rev3:** selecionar sobre pares `(schema, executable_query_plans)` e recursos físicos, mantendo ativação, cobertura cruzada e fontes canônicas como decisões distintas.
4. **Dataset novo:** várias seeds Java, mais sensores e dispositivos, intervalos diversos, esparsidade, histórico longo e grupos por duração; incluir Q1 com resposta positiva, várias instâncias de Q5 e Q8 e carga de escrita/atualizações.
5. **Congelar antes da medição:** registrar hashes, seeds, perfis, scripts, consulta/plano/candidato, pesos e regras de seleção. Avaliar em campanhas independentes contra baseline otimizada.
6. **Escalabilidade TS2-U vs TS2-D:** identificar se e em qual escala a redução de partições compensa os custos de fan-out e o overhead de armazenamento de TS2-D. Não extrapolar o cenário atual de 6 dispositivos/2 sensores para perfis maiores.

### Observação sobre o histórico

Na Rev2, **35 das 36 rotas** tiveram contagens instrumentadas coerentes com o modelo. A exceção foi Q8/TS0 (**2 previstas, 3.000 iteradas**). O E5 é justamente o experimento controlado para verificar essa exceção, preservando todo o histórico anterior.
