# SchemaLens V4 Rev2 — recuperação segura do holdout Java

## Diagnóstico do log recebido (26/10/2026)

1. Os **11 testes unitários** da Rev2 passaram.
2. `FileExistsError: Rev2 freeze already exists` sobre `EXP_OLD` é **esperado**: o congelamento exploratório Rev2 já existe. A execução seguinte, `--verify-only`, mostrou `PASS`. **Não recriar o Rev2 antigo.**
3. `Workspace already exists` é uma proteção válida. O script `prepare_holdout.py` não retoma nem sobrescreve tentativas anteriores.
4. O run Java novo tem uma pasta `.../java_work/runs/v3_.../schemalens`, mas **não tem `outputs/stage8/32_frozen_manifest.json`**. O `run_rev2.sh` e o `run_confirmatory.sh` exigem esse congelamento do V3 nativo *novo*, e por isso ambos abortam antes de Cassandra.
5. Não recebemos os logs originais `run_status.json` e `logs/prebenchmark.log`; portanto ainda não é possível afirmar qual comando dentro de A1/Stages 4–8 falhou. Este pacote inspeciona esses logs e tenta concluir a etapa pré-benchmark em uma **nova pasta isolada** aproveitando os mesmos bytes de CSV e da captura Java **independentes** do V3 histórico.

**Não renomeie, apague nem edite o workspace parcial, a captura, o HBench antigo ou os scripts de seleção.**

## 1. Instalar e rodar os testes

Na raiz `SchemaLens_IoT_Cassandra_V3_Automatico`:

```bash
unzip -o SchemaLens_V4_Rev2_Holdout_Recovery.zip
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"
PATCH="$(realpath SchemaLens_V4_Rev2_Holdout_Recovery)"
EXP_OLD="$(realpath native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748)"
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
python3 -m unittest discover -s "$PATCH/tests" -p 'test_*.py' -v
```

## 2. Inspecionar o erro original sem modificar arquivos

```bash
python3 "$PATCH/recover_holdout.py" \
  --workspace "$WORK" \
  --reference-v3 "$EXP_OLD" \
  --mode inspect
```

Isto mostra o estado de geração e as últimas linhas de `run_status.json`, `logs/prebenchmark.log`, `logs/native_generation.log` e `logs/capture.log`.

Se `native_queries.jsonl`, `native_data/schema.txt`, `native_data/info.txt` ou `BigBatch_*.csv` estiverem ausentes, a geração Java está incompleta. **Não avance ao congelamento.** Nesse caso, envie os logs para diagnosticar a falha do gerador; uma nova geração deve usar outro diretório de workspace, nunca apagar este.

## 3. Recuperar o Stage 8 do experimento novo

Verifique primeiro se os artefatos Java foram produzidos:

```bash
python3 "$PATCH/recover_holdout.py" \
  --workspace "$WORK" --reference-v3 "$EXP_OLD" \
  --mode recover --dry-run
```

Depois:

```bash
python3 "$PATCH/recover_holdout.py" \
  --workspace "$WORK" --reference-v3 "$EXP_OLD" \
  --mode recover
```

Este procedimento:

- valida a pré-inscrição das seeds (`DATA_SEED`, `QUERY_SEED`, `POINT_STEP`, etc.), o commit Java fixado, os hashes do gerador/adaptador e as **12 classes** da captura Java;
- exige que o SHA-256 da captura nova não seja idêntico ao histórico;
- copia o código isolado para `java_work/runs/v3_.../schemalens_rev2_recovered`, **sem alterar** a antiga pasta `schemalens` que falhou;
- reexecuta A1 e Stages 4–8 **sem Cassandra** com os CSVs Java e a captura já gerados;
- exige congelamento V3 íntegro, valida que os novos dados e consultas são diferentes do histórico e cria um alias de bytes idênticos para compatibilidade com a Rev2;
- registra `rev2_recovery_prebenchmark.log` e `REV2_RECOVERY_MANIFEST.json` no run Java, sem sobrescrever arquivos preexistentes.

**Importante:** se o runner original continuar falhando, a recuperação termina com erro. O log novo explicará a causa. Não é um script para mascarar erros de implementação.

## 4. Somente após `SUCCESS: valid new V3 Stage8`

Defina `NEW_EXP` usando o caminho **impresso pelo recuperador**. Se o workspace tiver apenas um `v3_*`:

```bash
RUN="$(find "$WORK/java_work/runs" -mindepth 1 -maxdepth 1 -type d -name 'v3_*' -print -quit)"
NEW_EXP="$RUN/schemalens_rev2_recovered"
test -f "$NEW_EXP/outputs/stage8/32_frozen_manifest.json" || { echo 'Stage8 ausente; não executar benchmark'; exit 1; }

bash "$ADDON/run_rev2.sh" "$NEW_EXP" \
  --confirmatory --reference-v3 "$EXP_OLD"

bash "$ADDON/run_rev2.sh" "$NEW_EXP" --verify-only
```

Esse novo HBench Rev2 é congelado **antes** de qualquer medição. Se ele já existir, `FileExistsError` é a proteção esperada: use `--verify-only`, não sobrescreva.

## 5. Benchmark Cassandra — apenas após o congelamento confirmado

```bash
bash "$ADDON/run_confirmatory.sh" "$NEW_EXP" \
  --host 127.0.0.1 --port 9043 \
  --rounds 40 --warmup 5 --page-size 16 \
  --measurement-seed 80421 \
  --confirm-reset RESET_ONLY_SCHEMA_CONF_R2_KEYSPACES
```

Execute somente em instância Cassandra descartável; esse comando modifica/recria *apenas* o namespace confirmatório especificado pelo runner. Ele não executa se os hashes da Rev2 divergirem.

## 6. Se houver nova falha

Não repita a recuperação no mesmo destino nem delete arquivos. Envie:

- saída de `--mode inspect`;
- `"$RUN/run_status.json"`;
- as últimas 100 linhas de `"$RUN/logs/prebenchmark.log"`;
- se chegou à recuperação, as últimas 100 linhas de `"$RUN/rev2_recovery_prebenchmark.log"`.

### Limitações científicas

- A nova geração deve continuar sendo a do **Java IoT Benchmark fixado**, não a repetição sintética de dados do E3;
- os 12 objetos Java são uma amostra por classe e **não** equivalem a uma execução SQL oficial ou a um replay da frequência de operações;
- os Stages 7 e 8 Rev2 não podem consumir latências Stage 9;
- nenhum resultado confirmatório pode ser alegado antes da execução Cassandra na nova carga.
