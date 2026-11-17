# SchemaLens V4 Rev2 — recuperação do teste de contrato TS5/Q7

## Diagnóstico

No novo holdout Java, o `GROUP_BY_TIME_UNIT` de Q7/Q11 é **3.600.000 ms (1 hora)**. A política física da V3, herdada no experimento, mantém `rollup_window_ms=20.000` (20 segundos). O Stage 5 rejeita corretamente o encaminhamento `TS5` quando a consulta usa granularidade diferente daquela pré-agregada.

O teste histórico `test_06_aligned_rollup_allowed_with_right_granularity` contém uma hipótese específica do piloto antigo: usa a Q7 real da captura e só ajusta `end_ms_exclusive`, sem adaptar a sua granularidade nem alinhar o início da janela. Assim, quando a Q7 Java passa a agrupar por **uma hora**, o teste espera `CONDITIONAL` apesar de a rota ser `INELIGIBLE` segundo o contrato atual. **Não é regressão da TS5 e não deve ser corrigido alterando Stage 5 ou o Java.**

O log já informou `STOP BEFORE BENCHMARK: Stage8 HBench frozen` antes dessa falha. Portanto, em vez de regenerar novamente os dados ou refazer os Stages 4–8, este pacote:

1. Verifica a captura Java, seu pré-registro, os hashes do V3 histórico, a independência dos novos dados e o HBench Stage8 **já congelado** em `schemalens_rev2_groupby_aligned`.
2. Faz uma cópia íntegra em novo diretório `schemalens_rev2_ts5_contract_validated`. Não apaga nem sobrescreve nenhum outro diretório.
3. Modifica **apenas o teste** `tests/test_iot_stages_4_to_9.py`, substituindo um teste positivo que pressupunha `group_by_ms == rollup_window_ms` por um teste sintético *não vazio* de granularidade explicitamente alinhada. Também exige que a Q7 nativa de 1 hora continue inelegível para o TS5 de 20 segundos.
4. Executa as três suítes da V3 em modo offline, sem Cassandra. Confere que o HBench, manifesto Stage8 e os arquivos de entrada continuam intactos.
5. Cria o alias byte a byte da captura e um manifesto `REV2_TS5_TEST_CONTRACT_RECOVERY_MANIFEST.json` registrando exatamente a modificação, os hashes e o resultado da regressão.

**Não muda:** Java, A1, Stage 4, Stage 5, Stage 6, Stage 7, Stage 8, política analítica, frozen V3/Rev1, latências, nem as regras Rev2.

## 1. Instalar na raiz do projeto

```bash
unzip -o SchemaLens_V4_Rev2_TS5_TestContract_Fix.zip

PATCH="$(realpath SchemaLens_V4_Rev2_TS5_TestContract_Fix)"
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"
GROUPBY_PATCH="$(realpath SchemaLens_V4_Rev2_GroupBy_Fix)"
EXP_OLD="$(realpath native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748)"
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"

bash "$PATCH/run_tests.sh"
```

## 2. Simular e validar a recuperação

```bash
python3 "$PATCH/recover_test_contract.py" \
  --workspace "$WORK" \
  --run-dir "$RUN" \
  --reference-v3 "$EXP_OLD" \
  --groupby-patch "$GROUPBY_PATCH" \
  --dry-run
```

A simulação não cria arquivos. Se o HBench já congelado estiver válido e a versão do teste for a esperada, execute:

```bash
python3 "$PATCH/recover_test_contract.py" \
  --workspace "$WORK" \
  --run-dir "$RUN" \
  --reference-v3 "$EXP_OLD" \
  --groupby-patch "$GROUPBY_PATCH"
```

Resultado esperado:

```
SUCCESS: regenerated fixture contract tests passed; Stage8 SHA preserved
FRESH_EXPERIMENT=.../schemalens_rev2_ts5_contract_validated
No Cassandra used.
```

Se aparecer `VALIDATION FAILED` ou outro erro, **não execute Cassandra**. Encaminhe `$RUN/rev2_ts5_contract_recovery_tests.log` e a última mensagem de erro. O recuperador guarda a cópia tentada para auditoria e proíbe sobrescrita.

## 3. Congelar o HBench Rev2 confirmatório

Apenas se o passo anterior terminar com `SUCCESS`:

```bash
NEW_EXP="$RUN/schemalens_rev2_ts5_contract_validated"

bash "$ADDON/run_rev2.sh" "$NEW_EXP" \
  --confirmatory --reference-v3 "$EXP_OLD"

bash "$ADDON/run_rev2.sh" "$NEW_EXP" --verify-only
```

A proteção do congelamento Rev2 impede que se mude o HBench depois da seleção. O novo `R2H_*` deve existir antes do benchmark.

## 4. Avaliação confirmatória com Cassandra

Só após o `--verify-only` passar, em **Cassandra descartável**:

```bash
bash "$ADDON/run_confirmatory.sh" "$NEW_EXP" \
  --host 127.0.0.1 --port 9043 \
  --rounds 40 --warmup 5 --page-size 16 \
  --measurement-seed 80421 \
  --confirm-reset RESET_ONLY_SCHEMA_CONF_R2_KEYSPACES
```

Este runner pode recriar keyspaces próprios de confirmação. Não use banco com dados de produção.

## 5. Arquivos para enviar após o congelamento

- `$RUN/REV2_TS5_TEST_CONTRACT_RECOVERY_MANIFEST.json`
- `$RUN/rev2_ts5_contract_recovery_tests.log`
- `$NEW_EXP/outputs/v4_rev2_prebenchmark/stage8/28_hbench_v4_rev2_frozen.json`
- `$NEW_EXP/outputs/v4_rev2_prebenchmark/stage8/30_hbench_v4_rev2_summary.csv`
- `$NEW_EXP/outputs/v4_rev2_prebenchmark/stage8/manifest.json`

### Nota metodológica

O *holdout* Java original permanece pré-registrado e intacto. Esta correção resolve uma **asserção de teste dependente do dataset piloto**, detectada depois do congelamento do parent V3, mas **antes de qualquer medição Cassandra**. A modificação fica registrada como correção de infraestrutura/teste. O Stage 5 continua recusando usar um TS5 de 20 segundos para uma Q7 de 1 hora no roteamento atual; uma futura política de composição de rollups exigiria implementação e avaliação próprias, não uma flexibilização implícita.

### Validação executada antes da entrega

- 5 testes de integridade do patch.
- 20 testes de `test_iot_stages_4_to_9.py`, 7 testes de `test_stage4_rev2.py` e 6 de `test_official_bridge_v3.py` (33 no total), em cópia local de regressão.
- Teste isolado de Q7 com `group_by_ms=3600000` e `rollup_window_ms=20000`, comprovando o `INELIGIBLE` nativo e `CONDITIONAL` apenas após construir uma consulta **nova** semanticamente alinhada e não vazia.
- Ainda não foi executado contra a pasta Java do seu computador.
