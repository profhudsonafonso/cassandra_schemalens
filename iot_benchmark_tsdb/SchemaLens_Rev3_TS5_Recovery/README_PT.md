# SchemaLens Rev3 — recuperação final do teste TS5/Q7 (campanha A)

## Por que o pipeline parou?

Após alinhar a cópia isolada do cenário ao valor Java pré-registrado `GROUP_BY_TIME_UNIT=3600000` ms, a execução da campanha A de 05/11/2026 alcançou o Stage 8. O único teste histórico que falhou foi:

```
test_06_aligned_rollup_allowed_with_right_granularity
AssertionError: 'INELIGIBLE' != 'CONDITIONAL'
```

O TS5 tem `rollup_window_ms=20000` ms, mas Q7/Q11 do novo Java usam agrupamento de 3600000 ms. O `route_family` da V3 *não implementa* composição de rollups de 20s para janelas de 1h. Por isso a consulta Java nativa precisa ser `INELIGIBLE` nesse contrato. O teste antigo reutilizava Q7 Java como caso positivo de granularidade alinhada e ficou inadequado. Ele é substituído **apenas numa cópia separada**, por (i) negação da elegibilidade nativa e (ii) um caso sintético, não vazio e alinhado de 20s que precisa ser `CONDITIONAL` e produzir resultado correto.

## Garantias da correção

- Não gera novos dados Java, não muda seeds, consultas, propriedades Java nem pré-registro.
- Não muda `iot_pipeline/stage5.py`, Stages 7/8, política física, custos, latências ou HBench.
- Preserva `schemalens` (falha original) e `schemalens_rev3_groupby_aligned_A` (congelamento com teste obsoleto).
- Copia o Stage 8 já congelado e confere novamente seus SHA-256, antes e depois dos testes.
- Executa três suites de regressão offline. Após sucesso e verificações de independência, cria os manifests/aliases que permitem `A freeze` e `A preflight` pelo runner original.
- Se ocorrer outra falha, a nova cópia e o log permanecem preservados; não se faz nova tentativa nesse mesmo destino.
- Não faz chamadas ao Cassandra; `A benchmark` continua bloqueado até a confirmação do preflight.

## Comandos no Ubuntu

No mesmo diretório usado antes:

```bash
cd ~/Documents/schemalens_cassandra/SchemaLens_IoT_Cassandra_V3_Automatico
source ~/Documents/schemalens_cassandra/venv/bin/activate
unzip -o SchemaLens_Rev3_TS5_Recovery.zip
FIX="$(realpath SchemaLens_Rev3_TS5_Recovery)"
GROUPBY_PATCH="$(realpath SchemaLens_Rev3_GroupBy_Recovery)"
PACK="$(realpath SchemaLens_Rev3_Independent_Confirmatory)"
bash "$FIX/run_tests.sh"

export WORKSPACE_OVERRIDE="/home/hudson/Documents/schemalens_cassandra/schemalens_rev3_new_java_A_retry_20261105_152344"
RUN="$WORKSPACE_OVERRIDE/java_work/runs/v3_20261105T182346Z_pid3444580"
V3_OLD="$(realpath native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748)"
REV2_OLD="$(realpath -m ../schemalens_confirmatorio_rev2/java_work/runs/v3_20261026T200853Z_pid4169024/schemalens_rev2_ts5_contract_validated)"

FIX_OPTS=(
  --campaign A
  --workspace "$WORKSPACE_OVERRIDE"
  --run-dir "$RUN"
  --reference-v3 "$V3_OLD"
  --reference-rev2 "$REV2_OLD"
  --source-project "$PWD"
  --groupby-patch "$GROUPBY_PATCH"
  --addon "$PACK"
  --rev2-addon "$PWD/SchemaLens_V4_Rev2_Confirmatory"
  --rev3-addon "$PWD/SchemaLens_V4_Rev3_PlanAware"
)

python3 "$FIX/finish_recovery.py" "${FIX_OPTS[@]}" --dry-run
python3 "$FIX/finish_recovery.py" "${FIX_OPTS[@]}"
```

**Sucesso esperado**:

```
SUCCESS: Java data reused; original V3 Stage8 SHA256 preserved; TS5 tests valid.
FRESH_EXPERIMENT=.../schemalens_rev3_ts5_contract_validated_A
No Cassandra used. Next: run_campaign.sh A freeze; preflight.
```

A recuperação só considera concluído depois desse `SUCCESS`. O script cria:

```
$RUN/schemalens_rev3_ts5_contract_validated_A/
$RUN/rev3_ts5_contract_A_tests.log
$RUN/REV3_TS5_CONTRACT_A_MANIFEST.json
$WORKSPACE_OVERRIDE/HOLDOUT_GENERATION_OUTPUT.json
$WORKSPACE_OVERRIDE/REV3_NEW_CAMPAIGN_REGISTRY.json
```

## Congelar e validar o plano Rev3 — sem Cassandra

Rode **apenas após `SUCCESS`**:

```bash
bash "$PACK/run_campaign.sh" A freeze
bash "$PACK/run_campaign.sh" A preflight
```

Se preflight funcionar, pode iniciar o benchmark em Cassandra *descartável*, conforme o protocolo já estabelecido:

```bash
bash "$PACK/run_campaign.sh" A benchmark
```

Não execute `generate` novamente. O `run_status.json` da geração que falhou pode continuar mostrando `prebenchmark=FAILED`, pois registra aquela tentativa histórica; a recuperação é identificada por manifests independentes.

## Se algum comando falhar

Se `finish_recovery.py` falhar, **não repita** o comando de recuperação sobre a mesma pasta. Execute:

```bash
tail -n 100 "$RUN/rev3_ts5_contract_A_tests.log"
```

Se o erro ocorrer em `A freeze` ou `A preflight`, envie as mensagens completas do traceback. Não altere os artefatos congelados.

## Escopo científico

A correção não introduz composição automática de TS5 de 20 segundos em Q7 de 1 hora. Tal plano exigiria código e um novo contrato Stage 5/7/9 e seria objeto de outra revisão metodológica. Este pacote é exclusivamente uma correção de teste/infrastrutura **anterior às medições**.
