# Recuperação segura do GROUP_BY_TIME_UNIT — SchemaLens Cassandra Rev3

## Diagnóstico do run A em 05/11/2026

O `run_status.json` recebido informa `build`, `generate`, `capture`, `isolate` = `OK`, e `prebenchmark` = `FAILED`. O `prebenchmark.log` termina **antes da materialização Cassandra**:

```
ValueError: Property/scenario mismatch: GROUP_BY_TIME_UNIT
```

O cenário A1 da cópia fonte usa `group_by_time_unit_ms=20000` (20 segundos), mas o gerador Java desta campanha foi pré-configurado para `GROUP_BY_TIME_UNIT=3600000` (uma hora). **Não altere** o gerador Java, os CSVs, as capturas, os seeds, o pré-registro, os Stages 7/8 ou os resultados de avaliações anteriores. O valor de 1 hora é o definido previamente para a campanha A. A recuperação só reconcilia o cenário analítico **numa nova cópia isolada**.

## O que o reparo faz

1. Confere os seeds e escala exatos pré-definidos de A/B, SHA-256 do HBench V3 de referência, hashes do código gerador/adaptador e o commit IoT Benchmark fixado.
2. Confere que configuração Java, pré-registro e os demais campos semânticos do cenário são coerentes; rejeita propriedades duplicadas ou alterações de dados.
3. Mantém intocadas a pasta `schemalens` que falhou, o pré-registro, `native_data`, `native_queries.jsonl` e `schema_project_isolated`.
4. Cria uma **nova** pasta `schemalens_rev3_groupby_aligned_A` e altera nela **somente** `group_by_time_unit_ms` para 3600000.
5. Reexecuta A1 até o Stage 8 V3 (somente offline). Se falhar, escreve um log e não autoriza `freeze`/`benchmark`.
6. Confere o congelamento V3, a independência de dados/consultas, a captura Java; cria um alias com bytes idênticos, `HOLDOUT_GENERATION_OUTPUT.json`, `REV3_NEW_CAMPAIGN_REGISTRY.json` e um manifesto de recuperação.
7. Depois do sucesso, o runner original da Rev3 pode executar `freeze` e `preflight`, **sem repetir o gerador Java**. O HBench Rev3 é criado e selado só por esse runner original, antes de qualquer benchmark.

O script não acessa Cassandra e não altera o HBench Rev2/Rev3 anterior.

## Instalar e testar

Na raiz do projeto:

```bash
cd ~/Documents/schemalens_cassandra/SchemaLens_IoT_Cassandra_V3_Automatico
source ~/Documents/schemalens_cassandra/venv/bin/activate
unzip -o SchemaLens_Rev3_GroupBy_Recovery.zip
PATCH="$(realpath SchemaLens_Rev3_GroupBy_Recovery)"
PACK="$(realpath SchemaLens_Rev3_Independent_Confirmatory)"
bash "$PATCH/run_tests.sh"
```

## Informar os caminhos já existentes (run A do usuário)

```bash
export WORKSPACE_OVERRIDE="/home/hudson/Documents/schemalens_cassandra/schemalens_rev3_new_java_A_retry_20261105_152344"

RUN="$WORKSPACE_OVERRIDE/java_work/runs/v3_20261105T182346Z_pid3444580"
V3_OLD="$(realpath native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748)"
REV2_OLD="$(realpath -m ../schemalens_confirmatorio_rev2/java_work/runs/v3_20261026T200853Z_pid4169024/schemalens_rev2_ts5_contract_validated)"
```

## Verificação sem escrever arquivos

```bash
python3 "$PATCH/recover_rev3.py" \
  --campaign A \
  --workspace "$WORKSPACE_OVERRIDE" \
  --source-project "$PWD" \
  --reference-v3 "$V3_OLD" \
  --reference-rev2 "$REV2_OLD" \
  --addon "$PACK" \
  --rev2-addon "$PWD/SchemaLens_V4_Rev2_Confirmatory" \
  --rev3-addon "$PWD/SchemaLens_V4_Rev3_PlanAware" \
  --run-dir "$RUN" \
  --dry-run
```

Só prossiga se a verificação mostrar `DRY RUN` e os valores esperados `20000 -> 3600000`.

## Recuperar o mesmo dataset Java em uma nova pasta

```bash
python3 "$PATCH/recover_rev3.py" \
  --campaign A \
  --workspace "$WORKSPACE_OVERRIDE" \
  --source-project "$PWD" \
  --reference-v3 "$V3_OLD" \
  --reference-rev2 "$REV2_OLD" \
  --addon "$PACK" \
  --rev2-addon "$PWD/SchemaLens_V4_Rev2_Confirmatory" \
  --rev3-addon "$PWD/SchemaLens_V4_Rev3_PlanAware" \
  --run-dir "$RUN"
```

Em caso de sucesso aparecerá `SUCCESS: Java data reused and new V3 Stage8 frozen; no Cassandra used` e o caminho `FRESH_EXPERIMENT`.

Em caso de falha, **não rode recuperação outra vez no mesmo destino nem delete arquivos**. Envie o final deste log:

```bash
tail -n 100 "$RUN/rev3_groupby_recovery_A_prebenchmark.log"
```

## Continuar a campanha confirmatória Rev3

Apenas após a recuperação acima terminar com `SUCCESS`:

```bash
bash "$PACK/run_campaign.sh" A freeze
bash "$PACK/run_campaign.sh" A preflight
```

O congelamento Rev3 será feito com os seeds, 60 repetições e 5 warmups pré-definidos pelo runner original. **Só depois de sucesso nas duas etapas**, e numa instância Cassandra descartável de pesquisa, execute:

```bash
bash "$PACK/run_campaign.sh" A benchmark
```

Depois do benchmark, siga o procedimento existente do manual de Rev3 para `show-flush`, `storage`, `pack`.

## Notas científicas

- O ajuste de cenário corrige uma **incompatibilidade de configuração do A1** com um valor de Java já definido no pré-registro. Não é calibração de custo, seleção retroativa ou alteração de carga.
- O V3 parent novo precisa ser reconstruído porque a primeira tentativa não produziu Stage 8. O HBench Rev2/Rev3 novo só será congelado após validação, sem latência observada.
- O valor de 1 h deve ser propagado às consultas Q7 e Q11 sem reduzi-lo silenciosamente ao rollup TS5 de 20 s. Caso um teste de contrato TS5 do runner V3 antigo falhe, reporte-o; não edite a definição de consulta ou o custo para fazer passar.
- Não usar o benchmark oficial de IoTDB como interpretação destes resultados: o projeto captura 12 **objetos de consulta** Java, não uma execução SQL oficial nem frequência de workload.
- Não declare a recuperação concluída até que apareça `SUCCESS` e o `A preflight` passe.
