# SchemaLens Cassandra — Etapa 3: Telemetria TS0 e fechamento da auditoria Rev2

## Objetivo

O experimento Rev2 já está **congelado**. A auditoria anterior confirmou **24/24 rotas instrumentadas** com previsões de `Session.execute` e `iterated_rows` consistentes; faltaram **12 rotas TS0**, pois o executor histórico retorna `None` nesses dois campos.

Este add-on é um **instrumentador pós-hoc (após a avaliação)**, NÃO uma nova seleção, nem uma nova execução para comparar latências. Ele reproduz as **12 rotas TS0 exatamente como congeladas**, contando:

- `execute_calls`: número de invocações explícitas a `session.execute` pelo executor V3 Stage9, **não** o total de round-trips de rede causado por paginação do driver;
- `iterated_rows`: objetos de linha entregues pelo driver e iterados pelo Python, **não** linhas lidas internamente no servidor, tombstones ou bytes reais;
- `oracle_correct`: equivalência dos resultados com o mesmo oráculo lógico da geração Java, verificada em cada execução.

A ferramenta **não altera código V3/Rev2, coeficientes, seleção, nem os CSVs confirmatórios**. Rejeita qualquer comando CQL que não seja um dos dois `SELECT` da TS0 registrados em V3 Stage9. Usa somente o keyspace já existente registrado no manifesto confirmatório. Não cria, apaga nem altera tabelas, não executa `flush` ou `nodetool`.

## 1. Instalar e testar

Execute na raiz do projeto `SchemaLens_IoT_Cassandra_V3_Automatico`:

```bash
unzip -o SchemaLens_Rev2_TS0_Telemetry.zip

TELEM="$(realpath SchemaLens_Rev2_TS0_Telemetry)"
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"

bash "$TELEM/run_tests.sh"
```

Os testes offline não usam Cassandra e devem terminar em `Ran 11 tests` / `OK`.

## 2. Medir somente a TS0 existente

**Pré-condições:** Cassandra experimental disponível em `127.0.0.1:9043`; keyspace `sl_conf_r2_15eadb0f_r2h_6355f4e57096` e tabela `ts0_iot_raw` ainda presentes; arquivos Rev2 originais sem modificações. Os testes pós-*flush* anteriores não exigem reset. Se a tabela tiver sido apagada, **não rode o benchmark antigo apenas para repô-la** sem planejar uma nova campanha: isso alteraria o estado experimental.

```bash
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"

NEW_EXP="$RUN/schemalens_rev2_ts5_contract_validated"
RESULTS="$NEW_EXP/outputs/v4_rev2_confirmatory/batch_15eadb0f"

OUT="$(realpath -m ../schemalens_rev2_ts0_telemetry_$(date +%Y%m%d_%H%M%S))"

bash "$TELEM/run_ts0.sh" "$NEW_EXP" \
  --results "$RESULTS" \
  --addon "$ADDON" \
  --out "$OUT" \
  --host 127.0.0.1 \
  --port 9043 \
  --rounds 3
```

O instrumentador executa primeiro `P.verify()` (hashes dos Stages 4–8 e novo Java holdout) e confirma que o HBench indicado no manifesto medido corresponde ao congelado. Todas as 12 rotas TS0 são validadas com o oráculo antes de produzir o relatório. **Não recolhe p95/p99** nem reescreve o histórico.

## 3. Conteúdo da saída

- `ts0_counts_raw.csv`: 36 registros (12 consultas × 3 repetições), chamadas, linhas iteradas e status do oráculo;
- `ts0_stage7_vs_observed.csv`: 12 registros, previsão Stage 7 congelada, observação e diferenças;
- `manifest.json`: hashes, ID TS0, keyspace, limites e status;
- `RELATORIO_TS0_PT.md`: tabela por consulta e interpretação científica.

As classificações por consulta são `MATCH`, `MISMATCH` ou `VARIABLE_OBSERVED_COUNTS`. Um *mismatch* é evidência útil para uma **futura** revisão. Não reajuste a Rev2 atual.

### Atenção especial a Q8

No executor `iot_pipeline/stage9.py` V3, TS0 executa leitura **sem restrição temporal** em Q8 e consome todas as linhas históricas por dispositivo e sensor. No proxy `v4_rev2/cost.py`, a rama TS0 de Q8 soma **uma linha por sensor** na variável `phys_rows`. Essas são semânticas distintas: esperamos uma discrepância entre linhas previstas e realmente iteradas na Q8, a confirmar no banco. Por exemplo, se há duas séries de 1.500 timestamps, o executor poderá iterar ~3.000 linhas, enquanto o proxy contabiliza 2. Não significa erro no resultado lógico; significa subestimação do custo de leitura da TS0.

Não trocar o plano da Q8 nem alterar o proxy congelado nesta etapa. Registrar a divergência e propor revisão futura **Rev3**, a ser avaliada com dados ainda não utilizados.

## 4. Compactar resultados

```bash
cd "$(dirname "$OUT")"
zip -r stage3_ts0_telemetry_results.zip "$(basename "$OUT")"
```

Envie `stage3_ts0_telemetry_results.zip` para analisar os 12 planos TS0, documentar divergências e encerrar a parte de contagens da auditoria Rev2.

## 5. Próximo estudo: crescimento real (sem geração nesta etapa)

Depois de concluir as 36 rotas da Rev2, executar campanhas futuras com novos seeds e parâmetros Java *pré-registrados*. Exemplo de matriz experimental prospectiva:

| Fator | Valores iniciais candidatos | Motivo |
|---|---|---|
| `DEVICE_NUMBER` | 6, 24 | Múltiplas partições |
| Sensores por dispositivo | 2, 8 | Densidade da linha TS2 |
| Loops × batch size | 300×5, 900×5, 2700×5 | Crescimento da série |
| `POINT_STEP_MS` | 600000 | Mesmo espaçamento entre medições |
| Seeds de dados | 31001, 31002, 31003 | Replicações independentes |
| Intervalo de consultas | 1 h, 1 d, 7 d, full-history | Varredura e buckets |
| Q8 com valores nulos | Completo e esparso | Semântica de última observação |

**Observação:** números são *proposta*, não execução confirmada. A combinação de 6 dispositivos, 8 sensores e 2700×5 produz **648.000 células**, e o intervalo de 13.500 amostras a cada 10 minutos cobre aproximadamente 94 dias. O limite `--max-cells` de leitores existentes pode precisar ser aumentado, sem mudar os critérios de seleção. A extrapolação de partições de dezenas de KiB para limiares de 32/128 MiB exige escalas muito maiores; nenhuma das campanhas pequenas estabelece um ponto de cruzamento favorável a TS2-D.

A nova campanha precisa congelar **outra** revisão, preservar a Rev2 como evidência histórica e registrar armazenamento pós-flush, acesso físico, manutenção de escrita e latência sob mesmo controle de execução. Para comparar latências de forma confirmatória, selecionar antes do benchmark e usar dados independentes; não derivar o novo HBench a partir dos p95 já observados.

## 6. Interpretação para o artigo

A Rev2 mostrou que a fonte canônica TS2-U foi selecionada por Pareto, a TS2-D e TS0 por diversidade, e que as 24 rotas TS2 estavam consistentes nas contagens auditadas. A medição pós-flush revelou 125.542 bytes em TS2-U, 130.360 bytes em TS2-D e 212.113 bytes em TS0. A telemetria TS0 da presente ferramenta completa a observabilidade do executor, mas não prova que o custo Rev2 prediz latência/bytes físicos. O estudo de crescimento será necessário para determinar quando buckets diários passam a valer a pena.
