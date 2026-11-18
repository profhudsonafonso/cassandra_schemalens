# SchemaLens IoT Cassandra V3 — E1, E2, E3 (pós-HBench congelado)

**Escopo:** experimentos adicionais de diagnóstico. A execução original de Stage4–8 **não é modificada** e o HBench **não é re-selecionado**. Hash esperado:

`2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb`

**Requisitos:** ambiente Python (`venv`) do projeto, `cassandra-driver`, Cassandra rodando na mesma porta usada no Stage9, e **os cinco keyspaces do Stage9 ainda presentes**. Não execute E1/E2 após apagar aqueles keyspaces; o pacote não os reconstitui automaticamente. O código do pacote usa apenas standard library para as análises, além das dependências já utilizadas no Stage9 da V3.

## Instalação

Descompactar o ZIP em `~/Documents/schemalens_cassandra/SchemaLens_IoT_Cassandra_V3_Automatico` de modo que a pasta `SchemaLens_Diagnostics_V3_E1_E2_E3` fique ao lado de `run_v3_auto.sh`.

Da raiz do projeto:

```bash
source ../venv/bin/activate  # apenas se seu venv ainda não estiver ativo
EXP="native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748"
ADDON="SchemaLens_Diagnostics_V3_E1_E2_E3"

bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode preflight
```

O preflight NÃO abre conexão Cassandra; verifica SHA-256, integridade de Stage4–8, existência dos manifestos Stage9 e origem Java dos 12 exemplos.

## Experimento E1 — decomposição de latência

```bash
bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e1 \
  --host 127.0.0.1 --port 9043 --warmup 10 --rounds 40
```

**Não escreve no Cassandra**. Lê cada uma das 12 consultas da baseline e dos quatro HBench, com seu **roteamento ORIGINAL**, e registra: latência total, `session.execute`+materialização do resultado, transformação Python, avaliação lógica, requisições CQL e linhas físicas retornadas. O campo `response_repr_bytes` é aproximação do tamanho da representação Python, NÃO os bytes de rede.

Arquivos: `outputs/diagnostics_v3_e123/e1/{e1_raw.csv,e1_summary.csv,e1_cql.csv,manifest.json}`.

> E1 utiliza uma sessão instrumentada que transforma cada `ResultSet` em lista para contabilizar todas as linhas. Portanto E1 é uma medição diagnóstica e NÃO uma repetição idêntica do Stage9 original. `db_fetch_ms` inclui round trip, driver e decodificação, NÃO a execução isolada no servidor.

## Experimento E2 — comparação física controlada BASELINE × TS2-U

```bash
bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e2 \
  --host 127.0.0.1 --port 9043 --warmup 10 --rounds 60 --seed 666
```

**Não escreve no Cassandra**. Antes de medir, examina as partições dos três dispositivos nas duas tabelas largas e comprova igualdade integral das 900 linhas e seus valores. Em seguida executa **as mesmas projeções, mesmos predicados, mesmo parser e mesmo avaliador Python** nas duas tabelas; o único identificador diferente nas consultas CQL é a tabela. Emparelha as medições da mesma classe, balanceando a ordem AB/BA em pares de rodadas com semente informada. Q12 utiliza limites próprios de cada operando, sem colapsar intervalos de duas subconsultas diferentes.

Arquivos: `outputs/diagnostics_v3_e123/e2/{e2_raw_pairs.csv,e2_summary.csv,e2_orders.csv,manifest.json}`.

**Cuidado:** E2 testa uma hipótese sobre as tabelas largas e o executor. A rota original de TS2-U, com `SELECT *` e restrições de cobertura, NÃO é substituída na configuração HBench.

## Experimento E3 — sensibilidade ao tempo / vários dias

E3 **cria e apaga somente keyspaces `sl_diag_e3_*`**, com confirmação explícita. **Não apaga `sl_iot_ref_native_v3` nem `sl_iot_h_*`.** Repete deterministicamente o dataset do Java original, deslocando timestamps por dias completos para produzir 1, 7 e 30 dias. É uma **extensão sintética**, NÃO novos dados gerados pelo Java.

| Escala | Células lógicas | Linhas largas (por dispositivo/instante) | Abordagem |
|---:|---:|---:|---|
| 1 dia | 1.800 | 900 | Controle |
| 7 dias | 12.600 | 6.300 | Cruzamento de buckets |
| 30 dias | 54.000 | 27.000 | Crescimento temporal |

Executar **uma escala por comando**:

```bash
bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e3 \
  --days 1 --host 127.0.0.1 --port 9043 --warmup 5 --rounds 30 \
  --seed 666 --confirm-diagnostic-reset RESET_DIAGNOSTIC_KEYSPACES_ONLY

bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e3 \
  --days 7 --host 127.0.0.1 --port 9043 --warmup 5 --rounds 30 \
  --seed 666 --confirm-diagnostic-reset RESET_DIAGNOSTIC_KEYSPACES_ONLY

bash "$ADDON/run_diagnostics.sh" --experiment "$EXP" --mode e3 \
  --days 30 --host 127.0.0.1 --port 9043 --warmup 5 --rounds 30 \
  --seed 666 --confirm-diagnostic-reset RESET_DIAGNOSTIC_KEYSPACES_ONLY
```

Para cada escala E3 instancia **uma baseline e os mesmos quatro HBench**, conservando os candidatos, rotas e DDL atuais (novos keyspaces de diagnóstico). Confere todas as respostas antes de medir. Depois balanceia a posição dos cinco projetos em blocos de cinco rodadas, com permutação inicial aleatória e semente fixa; mede as 12 consultas Java **com parâmetros originais fixos** e cinco novas sondas `EXTENDED_Q2/Q7/Q9/Q11/Q12_FULL_SPAN` com intervalos cobrindo todos os dias.

**Atenção:** as sondas são deliberadamente novas e não fazem parte da cobertura original validada no Stage8. Q12 derivada usa janela comum aos operandos, uma vez que o executor original HBench não contempla janelas distintas em todas as rotas. Qualquer resultado que não corresponda ao oráculo interrompe a medição daquela escala.

Arquivos separados: `outputs/diagnostics_v3_e123/e3_d01/`, `e3_d07/`, `e3_d30/`. O script não sobrescreve saídas existentes para evitar confundir repetições.

## Compilar relatório comparativo

Após terminar os experimentos:

```bash
PYTHONPATH="$ADDON" python3 -m diagnostic_e123.report \
  --results "$EXP/outputs/diagnostics_v3_e123"

zip -r E123_results.zip "$EXP/outputs/diagnostics_v3_e123"
```

Enviar `E123_results.zip` para análise estatística, gráficos e interpretação. **Nenhum resultado real de E1/E2/E3 foi medido neste pacote**; as medições devem ser executadas no Cassandra local pelo usuário.

## Decisões científicas que investigaremos

- **E1:** confirmar ou refutar que TS0 gasta mais tempo em requisições Cassandra e reconciliação de métricas que a baseline.
- **E2:** se baseline e TS2-U se aproximarem com CQL idêntico, a diferença do Stage9 anterior provavelmente se relaciona ao caminho do executor e/ou ao viés de medição, não simplesmente ao formato da tabela.
- **E3:** estudar se os custos de leitura das variantes sem bucket aumentam com o comprimento da série, e se TS2-D ganha em intervalos que atravessam dias.

Resultados não autorizam calibrar os pesos de Stage7/8 retroativamente. Novas escalas de número de dispositivos e sensores exigem contratos mais amplos e experimento separado.

## Testes unitários do pacote

```bash
bash "$ADDON/run_tests.sh" "$EXP"
```

Os testes utilizam sessões Cassandra simuladas (não executam `DROP`, `INSERT` ou medições). Caso queira inspecionar o código, os comentários são em inglês e estão em `diagnostic_e123/`.
