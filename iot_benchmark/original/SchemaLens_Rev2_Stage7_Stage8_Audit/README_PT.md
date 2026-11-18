# Auditoria quantitativa do Stage 7 e Stage 8 — SchemaLens Cassandra V4 Rev2

**Status:** auditor independente e somente leitura, preparado em 4 de novembro de 2026.  
**Objetivo:** comparar as **previsões congeladas antes do benchmark** com as **operações físicas registradas posteriormente**, sem recalibrar coeficientes, alterar o HBench ou rerodar o Cassandra.

## 1. O que está efetivamente disponível

Os seis arquivos da campanha confirmatória já foram auditados parcialmente, com os resultados abaixo:

- **2.400** medições de latência, **480** ordens completas de execução, **60** pares consulta/variante com p95 e p99 reproduzidos.
- Baseline otimizada: média dos 12 p95 = **2,3420 ms**.
- TS2-U `R2H_e72ef4d8529d`: **2,6729 ms**; sem Q5: **1,8974 ms**.
- TS2-D `R2H_bdc993acdde3`: **4,0364 ms**.
- TS0 `R2H_6355f4e57096`: **4,5154 ms**.
- Na Q5, o executor registrou **1** requisição CQL em TS2-U e **11** em TS2-D, com **1.500** linhas iteradas em ambas.
- O executor TS0 retornou `requests` e `iterated_rows` **vazios**. Isso NÃO equivale a zero.

**O que ainda não foi recebido no chat:** o diretório `outputs/v4_rev2_prebenchmark/` da **nova geração Java confirmatória** contendo as previsões por rota e os artefatos da seleção congelada. Por esse motivo, o relatório de exemplo entregue neste pacote é parcial, mesmo que as medições estejam completas. O comando abaixo gera a auditoria integral diretamente no seu Ubuntu.

## 2. O que o auditor compara

| Evidência antes da medição (Stage 7/8) | Evidência posterior (Cassandra) | Resultado |
|---|---|---|
| `request_count_proxy` | `raw_latencies.csv.requests` | `MATCH`, `MISMATCH`, `NOT_INSTRUMENTED` ou `VARIABLE_OBSERVED_COUNTS` |
| `physical_rows_proxy` | `raw_latencies.csv.iterated_rows` | Mesmos estados |
| `physical_writes_proxy` somado por deployment | `load.csv.physical_write_operations` | `MATCH`/`MISMATCH` |
| `analytic_read_proxy` | p95 da consulta | **Apenas comparação descritiva:** unidades de custo **não são milissegundos** |
| `returned_bytes_proxy` | Bytes reais | **NÃO MEDIDO** nesta campanha |
| `extra_bucket_fanout_proxy` | Requisições e buckets | Componente modelado, sem contraprova independente por partição |
| `max_partition_bytes_proxy` | Tamanho de partições/SSTables | **NÃO MEDIDO** nesta campanha |
| Pareto, scores, políticas, seleção e cobertura | HBench e manifests congelados | Recomputação algébrica + hashes |

**Importante:** `requests` são chamadas realizadas pelo executor CQL; `iterated_rows` conta linhas consumidas pelo driver Python. Nenhuma das duas equivale automaticamente a disco lido ou tempo do servidor Cassandra.

## 3. Instalar e testar

Coloque `SchemaLens_Rev2_Stage7_Stage8_Audit.zip` na pasta principal `SchemaLens_IoT_Cassandra_V3_Automatico` e execute:

```bash
unzip -o SchemaLens_Rev2_Stage7_Stage8_Audit.zip
AUDITOR="$(realpath SchemaLens_Rev2_Stage7_Stage8_Audit)"
python3 -m unittest discover -s "$AUDITOR/tests" -v
```

**Resultado local já obtido:** `Ran 10 tests ... OK`.

## 4. Rodar a auditoria integral sem refazer o benchmark

Os caminhos correspondem ao experimento confirmatório já usado neste chat:

```bash
# Execute na raiz de SchemaLens_IoT_Cassandra_V3_Automatico
AUDITOR="$(realpath SchemaLens_Rev2_Stage7_Stage8_Audit)"
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"
NEW_EXP="$RUN/schemalens_rev2_ts5_contract_validated"
RESULTS="$NEW_EXP/outputs/v4_rev2_confirmatory/batch_15eadb0f"

# Primeiro confira que o HBench da avaliação confirmatória existe:
test -f "$NEW_EXP/outputs/v4_rev2_prebenchmark/stage8/28_hbench_v4_rev2_frozen.json" || {
  echo "Falta o HBench Rev2 congelado em NEW_EXP"; exit 1;
}
test -f "$RESULTS/raw_latencies.csv" || {
  echo "Faltam os CSVs confirmatórios em RESULTS"; exit 1;
}

# Diretório de saída novo e FORA do experimento congelado:
OUT="$(realpath -m ../schemalens_rev2_stage78_audit_20261104)"
python3 "$AUDITOR/audit_stage78.py" \
  --experiment "$NEW_EXP" \
  --results "$RESULTS" \
  --addon "$ADDON" \
  --out "$OUT"
```

O auditor confere também os hashes dos módulos `v4_rev2/prebenchmark.py`, `v4_rev2/cost.py`, `v4_auto/selector.py`, `v4_rev2/benchmark.py` e `v4_rev2/runtime_helpers.py` do addon instalado. Caso tenham sido modificados após o congelamento, **a auditoria falha deliberadamente**, indicando o arquivo.

**Nenhuma conexão Cassandra é feita.** O programa não escreve em `NEW_EXP` nem em `RESULTS`, e recusa sobrescrever `OUT` existente.

Se o nome de `NEW_EXP` ou `RESULTS` for diferente no seu computador, substitua somente essas variáveis pelo caminho real. Não use o experimento de 1/7/30 dias ou o congelamento Rev1 como referência para a Rev2 confirmatória.

## 5. Arquivos produzidos

Se o congelamento estiver disponível, a execução criará:

```text
$OUT/
├── RELATORIO_AUDITORIA_PT.md
├── audit_summary.json
├── observed_by_query.csv
├── observed_aggregates.csv
├── stage7_predictions_vs_observations.csv
├── stage7_writes_vs_observed.csv
├── stage8_selection_audit.csv
└── frozen_stage7_policy_snapshot.json
```

O relatório confrontará todas as rotas selecionadas e marcará explicitamente as sem medição. Para cada candidato, é possível identificar em qual consulta o modelo previu número incorreto de requisições ou linhas. As razões `PARETO_FRONT` e `STRUCTURAL_CANONICAL_DIVERSITY_SAFEGUARD` também serão verificadas sobre os **seis covers** enumerados no congelamento.

Para compartilhar apenas os resultados da auditoria:

```bash
cd "$(dirname "$OUT")"
zip -r stage78_audit_rev2_resultados.zip "$(basename "$OUT")"
```

Envie `stage78_audit_rev2_resultados.zip` para a análise. **Não é necessário executar novamente Stage 9**.

## 6. Como interpretar `MATCH` e `MISMATCH`

Exemplo (contagens *observadas*; previsões reais serão carregadas pelo auditor):

| Consulta | TS2-U: requisições | TS2-D: requisições | TS2-U: linhas | TS2-D: linhas |
|---|---:|---:|---:|---:|
| Q2 | 1 | 2 | 144 | 144 |
| Q5 | 1 | 11 | 1500 | 1500 |
| Q8 | 1 | 1 | 1 | 1 |
| Q12 | 2 | 4 | 290 | 290 |

Se o Stage 7 estimar `11` requisições em TS2-D/Q5, o estado será `MATCH`. Se estimar `1`, será `MISMATCH`, e devemos verificar os limites temporais, a contagem de buckets, as regras de acesso à chave de partição e o executor.

Para TS0, o benchmark atual usa `iot_pipeline.stage9.execute_query` e **não retorna telemetria detalhada** para o auditor. Suas linhas aparecerão como `NOT_INSTRUMENTED`; a solução é instrumentar o executor **em uma nova campanha controlada**, sem alterar o arquivo de medições já concluído.

## 7. Critérios de decisão para a próxima etapa

1. **Integridade obrigatória:** todos os hashes Rev2, entradas V3, data/query hashes e os módulos do addon precisam conferir.
2. **Seleção explicável:** todos os deployments selecionados devem pertencer aos covers enumerados; rotas devem ser `DIRECT`; scores e Pareto devem ser reprodutíveis.
3. **Contagens físicas:** as rotas TS2 devem ter solicitações/linhas registradas e ser verificadas. `MISMATCH` será investigado, nunca ajustado silenciosamente.
4. **Escritas:** operações previstas e realizadas devem ser comparadas por deployment, sem confundir número de operações com throughput.
5. **Métricas ainda ausentes:** armazenamento efetivo, pressão de partição e bytes de transferência real exigem a Etapa 2.
6. **Congelamento:** nunca alterar pesos da Rev2 ou rotas após a medição e ainda chamá-la de validação confirmatória. Revisões exigem nova versão e novo holdout.

## 8. Metodologia resumida

**Stage 4:** um template é ativado quando há evidência semântica e de workload para sua existência.  
**Stage 5:** `Cover(c,q)` é verificado independentemente da consulta que justificou a materialização de `c`.  
**Stage 6:** define a materialização e as operações de escrita, considerando a fonte canônica.  
**Stage 7:** projeta custo físico por plano:

\[
\widehat{C}_{read}=4N_{req}+0.02N_{rows}+0.20V_{KiB}+2N_{extraBucket}+5P_{partition}.
\]

Esses coeficientes são hipóteses de engenharia predefinidas, não uma conversão direta para milissegundos.  
**Stage 8:** enumera fontes canônicas TS0, TS2-U e TS2-D com TS4 opcional, calcula custos de leitura/escrita/armazenamento, produz Pareto e retém diversidade estrutural explicitamente marcada.  
**Audit:** verifica se as estimativas correspondem às operações reais, **sem modificar o modelo**.

## 9. Próxima etapa após esta auditoria

A **Etapa 2** será medir armazenamento real (`system.size_estimates`, SSTables, `nodetool tablestats`), custo de escritas e tamanho das partições nas mesmas tabelas materiais já preservadas. Esta fase terá outro protocolo e será separada da auditoria offline para não confundir proxy com métrica física.

---

**Limitações dos dados confirmatórios:** 12 objetos Java (um por classe), dois sensores DOUBLE alinhados, Q1 sem resultados, sem replay oficial de SQL/IoTDB ou frequências do workload, uma campanha de 40 rodadas por consulta. Portanto, a avaliação demonstra viabilidade e evidência de competitividade, mas não constitui resultado oficial do IoT Benchmark.
