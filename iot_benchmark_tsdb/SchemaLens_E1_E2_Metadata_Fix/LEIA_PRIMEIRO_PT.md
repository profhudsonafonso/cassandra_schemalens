# SchemaLens E1/E2 — correção de metadados sem modificar HBench

## Diagnóstico

`cassandra.DriverException: Schema metadata was not refreshed` em `cluster.refresh_schema_metadata()` dentro de `ensure_tables()` do addon E123. E3 conseguiu ler e escrever nos keyspaces de diagnóstico, então a falha de E1/E2 não constitui evidência de falha geral do Cassandra. O driver pode falhar ao renovar o cache completo de schemas; E1/E2 só precisam conferir que tabelas originais existem.

## Aplicar da raiz de SchemaLens_IoT_Cassandra_V3_Automatico

```bash
unzip -o SchemaLens_E1_E2_Metadata_Fix.zip
python3 SchemaLens_E1_E2_Metadata_Fix/install_fix.py
EXP="native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748"
ADDON="SchemaLens_Diagnostics_V3_E1_E2_E3"
bash "$ADDON/run_tests.sh" "$EXP"
bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode preflight
```

A correção substitui **somente** a função `ensure_tables` do arquivo `SchemaLens_Diagnostics_V3_E1_E2_E3/diagnostic_e123/core.py`. O original recebe uma cópia backup. Usa `SELECT` em `system_schema.keyspaces` e `system_schema.tables`; não executa DDL. Não toca nos diretórios do experimento congelado, nos keyspaces ou nos CSVs E3.

## Retomar E1/E2

```bash
bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e1 --host 127.0.0.1 --port 9043 --warmup 10 --rounds 40
bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e2 --host 127.0.0.1 --port 9043 --warmup 10 --rounds 60 --seed 666
```

Ambos são **somente leitura**. O pacote original protege os outputs contra sobrescrita e mantém o HBench congelado; as medições E3 já existentes não devem ser refeitas. Se uma exceção nova aparecer, preserve a mensagem completa em vez de assumir outra causa.

## Após E1/E2

```bash
PYTHONPATH="$ADDON" python3 -m diagnostic_e123.report --results "$EXP/outputs/diagnostics_v3_e123"
zip -r E123_results_complete.zip "$EXP/outputs/diagnostics_v3_e123"
```

Envie o ZIP para analisar E1/E2 em conjunto com os resultados E3 já concluídos.
