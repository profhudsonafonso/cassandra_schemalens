# SchemaLens V4 — correção de importação do projeto isolado

## Problema identificado

A pasta externa `SchemaLens_IoT_Cassandra_V3_Automatico` também contém o pacote
`iot_pipeline`. O comando `python3 -m ...` executado nesse diretório insere o
próprio diretório de trabalho em `sys.path` antes de `PYTHONPATH`, por isso
carregava a versão externa, que não tem `native_reference_v3.py`. Isso
produzia `ModuleNotFoundError` nos seis testes e `Wrong imported iot_pipeline`.

## O que muda

Somente `SchemaLens_V4_Canonical_CrossReuse/run_tests.sh` e
`SchemaLens_V4_Canonical_CrossReuse/run_v4.sh`.

Os scripts entram na pasta isolada V3 antes de chamar Python, descartam
`PYTHONPATH` herdado e transformam o argumento `--experiment` em caminho
absoluto. A checagem rigorosa de importação (`ROOT == --experiment`) permanece.

Nenhum dado, arquivo congelado, executor ou família TS é modificado.

## Como instalar

Na pasta externa do projeto:

```bash
unzip -o SchemaLens_V4_Import_Path_Fix.zip

EXP="native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748"
ADDON="SchemaLens_V4_Canonical_CrossReuse"

bash "$ADDON/run_tests.sh" "$EXP"
bash "$ADDON/run_v4.sh" --experiment "$EXP" --mode offline --days 1
```

**Só depois que `PASS_OFFLINE` aparecer**, rode a medição ao vivo:

```bash
bash "$ADDON/run_v4.sh" \
  --experiment "$EXP" --mode live --days 1 \
  --host 127.0.0.1 --port 9043 --warmup 10 --rounds 50 \
  --seed 666 --page-size 16 \
  --confirm-reset RESET_V4_DIAGNOSTIC_KEYSPACES_ONLY
```

Esse último comando recria exclusivamente keyspaces `sl_diag_v4_d01_*`.
Requer Cassandra acessível na porta 9043 e uma instância de teste.
Não rerode se a pasta `outputs/diagnostics_v4/live_d01` já existir.

## Escopo da validação

O fix foi verificado em ambiente Python com um projeto externo contendo um
`iot_pipeline` conflitante: os 13 testes V4 passaram carregando o pacote da
pasta isolada, e o tratamento de argumentos relativos/absolutos foi testado.
O experimento nativo Java da máquina do usuário só pode ser validado no Ubuntu.
