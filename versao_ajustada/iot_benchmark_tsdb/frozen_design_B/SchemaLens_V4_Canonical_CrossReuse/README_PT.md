# SchemaLens V4 — E4 + canonical TS2 & cross-query reuse (experimental)

## Alcance, proveniência e proteção científica

Este é **um diagnóstico V4 novo**. Ele **não altera nem executa novamente** os Stages 1–8 da V3 e **não seleciona retroativamente** um HBench para V3. Usa as 12 amostras de objetos Java já capturadas, a mesma base nativa DOUBLE/ms e os quatro deployments V3 congelados, cujo SHA-256 deve ser `2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb`.

Os quatro designs V4 foram **pré-especificados a partir dos diagnósticos E1–E3** antes desta nova medição:

| Label | Fonte canônica | Q1–Q7,Q9–Q12 | Q8 | TS0 persistida? |
|---|---|---|---|---|
| V4_TS2U | TS2-U | TS2-U | TS2-U otimizada | Não |
| V4_TS2U_TS4 | TS2-U | TS2-U | TS4 | Não |
| V4_TS2D | TS2-D | TS2-D | TS2-D otimizada | Não |
| V4_TS2D_TS4 | TS2-D | TS2-D | TS4 | Não |

Também são avaliados sob o **mesmo protocolo intercalado** `BASELINE_ORIGINAL` (Q8 varre toda a partição) e `BASELINE_Q8_OPTIMIZED` (Q8 lê em ordem `timestamp DESC`, com `fetch_size=16` e parada após achar última observação não nula de **cada** sensor). Esses dois controles compartilham **exatamente a mesma tabela e dados** em um keyspace de diagnóstico. Assim, E4 isola a otimização do *plano de leitura*, sem alterar o esquema. Os quatro deployments V3 originais são instanciados **novamente em keyspaces de diagnóstico**, mas sua rota/conteúdo/DDL permanece congelado.

Não confundir: o *caminho de execução* V4 foi implementado especialmente para esta avaliação exploratória. O trabalho de desenvolver uma V4 acadêmica completa, com os Stages 4–8 reescritos para gerar automaticamente a mesma seleção, ainda é futuro. A proposta registra a cobertura cruzada semântica calculada antes de quaisquer latências V4; **não recalibra proxies analíticos nem pesos usando o resultado original da V3**.

## Pré-requisitos

- Linux + VS Code, Python 3.10+, `cassandra-driver` no ambiente Python usado anteriormente.
- O projeto `SchemaLens_IoT_Cassandra_V3_Automatico`, com a execução **isolada e congelada V3** e os scripts de `iot_pipeline.native_reference_v3` previamente instalados dentro dela.
- Cassandra de teste descartável, acesso em `127.0.0.1:9043`. Ao executar no seu Docker de pesquisa, nunca use uma instância de produção.
- Experimentos originais V3/Stage 9 mantidos; a nova carga usa somente `sl_diag_v4_d01_*`, `sl_diag_v4_d07_*` e `sl_diag_v4_d30_*`.

## 1. Instalar e rodar testes sem Cassandra

Coloque `SchemaLens_V4_Canonical_CrossReuse.zip` na raiz do projeto:

```bash
unzip -o SchemaLens_V4_Canonical_CrossReuse.zip
EXP="native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748"
ADDON="SchemaLens_V4_Canonical_CrossReuse"
bash "$ADDON/run_tests.sh" "$EXP"
```

## 2. Pré-registrar V4 (sem conexão ao Cassandra)

```bash
bash "$ADDON/run_v4.sh" --experiment "$EXP" --mode offline --days 1
```

Isso produz `outputs/diagnostics_v4/V4_PROPOSAL_d01.json` na **pasta isolada**. Inclui a cobertura 4×12, os planos de roteamento Q1–Q12, a origem Java e hashes. É uma proposta exploratória, **não o Stage8 V3 e nem uma nova alegação de pré-seleção independente**. O modo live recusa iniciar caso a proposta correspondente não exista ou tenha sido alterada.

## 3. E4 + V4 ao vivo — 1 dia (primeira comparação)

**Atenção:** o comando abaixo APAGA E RECRIA exclusivamente os novos keyspaces `sl_diag_v4_d01_*`. É necessário fornecer literalmente o token de confirmação. Não são alterados os keyspaces `sl_iot_ref_native_v3` nem `sl_iot_h_*`.

```bash
bash "$ADDON/run_v4.sh" --experiment "$EXP" --mode live --days 1 \
  --host 127.0.0.1 --port 9043 \
  --warmup 10 --rounds 50 --seed 666 --page-size 16 \
  --confirm-reset RESET_V4_DIAGNOSTIC_KEYSPACES_ONLY
```

Esse teste inclui **10 variantes**: baseline original, baseline Q8 otimizada, quatro V4 e quatro deployments V3 congelados. Antes das medições, **cada par variante/consulta é comparado à resposta do avaliador lógico original**. Ordem de medição embaralhada de modo reproduzível e invertida em pares sucessivos. Outputs só são criados se tudo passar.

A medição de Q8 registra *linhas iteradas*, e não bytes reais transferidos; o driver pode pré-carregar uma página maior que a quantidade iterada.

## 4. Estender o eixo temporal — 7 e 30 dias (dados sintéticos)

As escalas 7/30d criam cópias do CSV Java deslocadas em dias UTC. Elas não correspondem à geração nativa de novos workloads oficiais. Os 12 objetos de consulta Java originais permanecem com parâmetros fixos, e cinco sondas extra atravessam a extensão temporal, separadamente rotuladas.

```bash
for DAYS in 7 30; do
  bash "$ADDON/run_v4.sh" --experiment "$EXP" --mode offline --days "$DAYS" --extended-probes
  bash "$ADDON/run_v4.sh" --experiment "$EXP" --mode live --days "$DAYS" --extended-probes \
    --host 127.0.0.1 --port 9043 --warmup 5 --rounds 30 \
    --seed 666 --page-size 16 \
    --confirm-reset RESET_V4_DIAGNOSTIC_KEYSPACES_ONLY
done
```

**Nota:** o guard experimental TS2-U permite no máximo 50.000 linhas por dispositivo nessas escalas. Esse guard **não retroage** sobre o limite que havia sido fixado na V3; o risco de crescimento de partições continua sendo parte da avaliação.

## 5. Consolidar resultados

```bash
PYTHONPATH="$ADDON:$EXP" python3 -m diagnostic_v4.report --experiment "$EXP"
zip -r V4_E4_results.zip "$EXP/outputs/diagnostics_v4"
```

Envie `V4_E4_results.zip`. Relatórios: `V4_E4_REPORT_PT.md`, `v4_audited_comparison.csv`, além das medições brutas `v4_raw.csv` e manifestos por escala.

## O que verificaremos

1. **E4:** baseline com varredura completa versus baseline otimizada sobre a *mesma* tabela. Se o ganho de TS4 diminuir, parte da vantagem vista anteriormente era do executor de Q8.
2. **TS2-U como fonte canônica:** se há quase empate com a baseline otimizada quando ambas usam o mesmo modelo largo e o mesmo processamento, como sugeriu E2.
3. **Cobertura cruzada:** se todas as 12 classes podem responder via TS2 com a mesma semântica validada pelo avaliador original, agora também nas rotas que V3 fixava em TS0.
4. **TS2-D:** quanto custa acessar muitos buckets em consultas de intervalo completo.
5. **Redundância e armazenamento:** quantas inserções lógicas físicas V4 evita ao dispensar TS0; nenhum resultado de throughput oficial é inferido.

Limitações: não há replay de SQL ou frequências IoTDB oficiais; Q8/TS4 equivale ao último valor não nulo no snapshot estático e não cobre transações/atualizações atrasadas; tempos são de aplicação Python + driver, não apenas servidor; devem ser feitas repetições independentes em escala maior antes de atribuir significância às diferenças pequenas.

## Segurança e reproducibilidade

Os comandos não editam `iot_pipeline/stage4.py` a `stage9.py`, arquivos de A1 nem outputs `stage4`–`stage8`. Todo resultado V4 é armazenado em `outputs/diagnostics_v4` e o pacote compara os hashes V3 antes e depois do teste. **Não use os resultados V4 para declarar uma seleção V3 pré-benchmark.**
