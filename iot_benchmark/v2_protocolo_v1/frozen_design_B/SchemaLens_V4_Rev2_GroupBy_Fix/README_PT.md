# SchemaLens V4 Rev2 — correção de GROUP_BY_TIME_UNIT na avaliação confirmatória

## Causa comprovada pelo log

O gerador Java produziu dados e capturou 12 objetos de consultas com sucesso, mas o A1 da cópia isolada abortou em:

```
ValueError: Property/scenario mismatch: GROUP_BY_TIME_UNIT
```

No holdout registrado anteriormente, `java_config/config.properties` contém `GROUP_BY_TIME_UNIT=3600000` (1 hora), enquanto o `phase_a1/inputs/scenario.json` original declara `group_by_time_unit_ms=20000` (20 segundos). O A1 exige igualdade exata entre os dois.

**Não altere os parâmetros do Java para 20000, não edite os arquivos congelados e não apague o workspace.** A escolha `3600000` já constava do pré-registro, antes do benchmark Cassandra. Este patch permite que o cenário analítico da NOVA cópia isolada reflita a configuração Java efetivamente utilizada.

## O que o patch faz

1. Confere o SHA-256 do HBench V3 histórico, o pré-registro da geração, o código do gerador, o adaptador de referência, o CSV nativo e a captura Java.
2. Confere que `GROUP_BY_TIME_UNIT` no `config.properties` é idêntico ao pré-registro e que as demais propriedades que o A1 compara já coincidem com o cenário.
3. Cria uma pasta **nova**, `schemalens_rev2_groupby_aligned`, sem tocar nas tentativas anteriores `schemalens` e `schemalens_rev2_recovered`.
4. Atualiza **somente** `group_by_time_unit_ms` no `phase_a1/inputs/scenario.json` da nova cópia, guardando os hashes anterior/posterior e a justificativa.
5. Executa A1 -> Stage 8, sem Cassandra. Se falhar, guarda o log e impede passar ao benchmark.
6. Confere manifesto/hash do Stage 8 e independência de dados e parâmetros Java antes de criar o alias byte a byte da captura e o manifesto da recuperação.

## Comandos

A partir da raiz `SchemaLens_IoT_Cassandra_V3_Automatico`, com o ZIP extraído:

```bash
unzip -o SchemaLens_V4_Rev2_GroupBy_Fix.zip

PATCH="$(realpath SchemaLens_V4_Rev2_GroupBy_Fix)"
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"
EXP_OLD="$(realpath native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748)"
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"

bash "$PATCH/run_tests.sh"

python3 "$PATCH/recover_groupby.py" \
  --workspace "$WORK" --run-dir "$RUN" \
  --reference-v3 "$EXP_OLD" --mode recover --dry-run

python3 "$PATCH/recover_groupby.py" \
  --workspace "$WORK" --run-dir "$RUN" \
  --reference-v3 "$EXP_OLD" --mode recover
```

Quando a recuperação concluir, exibirá:

```
SUCCESS: valid new V3 Stage8 with GROUP_BY_TIME_UNIT aligned: ...
FRESH_EXPERIMENT=.../schemalens_rev2_groupby_aligned
```

Se essa mensagem NÃO aparecer, pare e inspecione:

```
$RUN/rev2_groupby_alignment_prebenchmark.log
```

Se a recuperação for bem-sucedida, congele a Rev2 **antes de medir**:

```bash
NEW_EXP="$RUN/schemalens_rev2_groupby_aligned"

bash "$ADDON/run_rev2.sh" "$NEW_EXP" \
  --confirmatory --reference-v3 "$EXP_OLD"

bash "$ADDON/run_rev2.sh" "$NEW_EXP" --verify-only
```

Somente após os dois comandos terem sucesso, rode a avaliação em Cassandra **descartável** (o comando recria apenas os seus keyspaces de confirmação):

```bash
bash "$ADDON/run_confirmatory.sh" "$NEW_EXP" \
  --host 127.0.0.1 --port 9043 \
  --rounds 40 --warmup 5 --page-size 16 \
  --measurement-seed 80421 \
  --confirm-reset RESET_ONLY_SCHEMA_CONF_R2_KEYSPACES
```

## Evidência gerada

- `rev2_groupby_alignment_prebenchmark.log`: log de A1–Stage 8.
- `REV2_GROUPBY_ALIGNMENT_MANIFEST.json`: hash da captura Java, valores/hashes anterior e posterior do cenário, hash do HBench V3 e da seleção Stage 8 recuperada.
- `schemalens_rev2_groupby_aligned/outputs/stage8/`: congelamento independente dos Stages 1–8.

Não apresentar esse reparo como uma nova parametrização retrospectiva; trata-se de **reconciliação do cenário analítico com um parâmetro Java de 1 hora pré-registrado antes de qualquer medição**. Arquivos e tentativas que falharam são mantidos.

## Validação realizada no ambiente de desenvolvimento

- 10 testes de regressão da recuperação, incluindo rejeição de divergência em outras propriedades, de alteração do pré-registro e de propriedades duplicadas.
- Execução real do A1 (sem captura Java) com a mesma configuração `GROUP_BY_TIME_UNIT=3600000` e cenário alinhado: PASS (fixture, 35 células).
- **Não** há teste ao vivo com os CSVs/captura independentes do seu Ubuntu nem benchmark Cassandra neste pacote; confirme o sucesso pelos logs da sua execução.
