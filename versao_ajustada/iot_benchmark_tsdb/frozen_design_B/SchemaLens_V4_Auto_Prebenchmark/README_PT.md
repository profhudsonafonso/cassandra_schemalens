# SchemaLens V4-AUTO: escolha automática da fonte canônica e cobertura cruzada

## Objetivo da etapa

Este complemento executa **somente a pré-seleção offline** no experimento nativo V3 já congelado. Ele evolui os Stages 4–8 para testar seleção de fonte canônica e reutilização entre consultas, e **não reabre** nem modifica a seleção anterior. Ele é um seletor V4 experimental independente — não substitui ainda o `iot_pipeline.stage8` da produção nem fornece um Stage 9 auto-executor.

As versões experimentais V4 com 1 e 30 dias foram projetadas **após** os diagnósticos E1–E3 e já tiveram resultados medidos. A nova seleção automática sobre o mesmo dataset é, portanto, exploratória. Para alegar confirmação independente, gerar novo dataset/captura Java com outra semente e registrar a seleção antes de medir os resultados dessa nova carga.

## Dados e famílias

- Entrada física: `outputs/stage4/10_candidate_specs.json` (Stage4 V3 **imutável**).
- Entrada de evidências: `phase_a1/outputs/activation_ts_only.json` (somente TS0–TS8).
- Entrada de consultas: `phase_a1/outputs/query_instances_pilot.json`, exigindo **Q1–Q12 com proveniência do exportador oficial Java**. Cada objeto possui hash da captura; os bytes do JSONL devem corresponder.
- Entrada de dados: `phase_a1/outputs/normalized_double_readings.csv` e regras originais de `config/pipeline_policy.json`.
- Fontes canônicas candidatas: TS0 (estreita), TS2-U (larga sem bucket), TS2-D (larga diária), **somente quando materializadas e ativadas pela A1 e Stage4 originais**.
- Materialização opcional TS4: última leitura, somente quando ativada e com validade de snapshot. Nenhuma TS9 é criada.
- Escopo intencional: não adicionamos TS1 ou TS5 como *fonte canônica* aqui. Essas famílias podem ser estudadas em outra revisão.

## Passo 1: descompactar e testar na raiz do projeto

Copie `SchemaLens_V4_Auto_Prebenchmark.zip` para a raiz do `SchemaLens_IoT_Cassandra_V3_Automatico` e execute:

```bash
unzip -o SchemaLens_V4_Auto_Prebenchmark.zip
bash SchemaLens_V4_Auto_Prebenchmark/run_tests.sh
```

## Passo 2: gerar novo HBench V4 automático, SEM CASSANDRA

```bash
EXP="native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748"

bash SchemaLens_V4_Auto_Prebenchmark/run_v4_auto.sh "$EXP"
```

O script muda para a pasta de experimento para garantir o import da cópia correta do pacote Python. O algoritmo:

1. Verifica hash fixo do HBench V3 `2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb` e confere TODOS os SHA-256 das entradas do manifesto congelado.
2. Confirma a captura real dos 12 objetos Java por SHA-256. Recusa o fixture ilustrativo.
3. Stage 4 V4: reatribui o papel canônico aos candidatos físicos previamente instanciados com seus guards de cardinalidade.
4. Stage 5 V4: valida cada par candidato/consulta com o avaliador lógico de A1. Inclui Q12 com intervalos independentes, Q5 sem filtro temporal e Q8 de última observação; usa cobertura cruzada sem alterar as ativações antigas.
5. Stage 6 V4: registra plano de cargas por candidata e origem canônica, além da limitação para escritas concorrentes.
6. Stage 7 V4: estima número de requisições por plano, linhas físicas lidas e custos lógicos de armazenamento/escritas usando os pesos **anteriores e congelados** da política, não tempos de execução.
7. Stage 8 V4: enumera fontes canônicas × TS4 opcional, calcula custos multicritério, Pareto e, no máximo, `top_k` representantes; preserva até um representante por fonte canônica sob salvaguarda de diversidade estrutural, identificando explicitamente os casos fora do Pareto.
8. Registra SHA-256 dos outputs e da própria implementação num manifesto V4. Recusa sobrescrever qualquer congelamento existente.

**Arquivos novos** (todos relativos à pasta isolada do experimento):

```text
outputs/v4_auto_prebenchmark/
  stage4/10_v4_candidate_specs.json
  stage4/11_not_instantiated.json
  stage5/15_cross_query_coverage.json
  stage5/16_cross_query_coverage.csv
  stage6/18_v4_write_plans.json
  stage7/21_storage_proxies.json
  stage7/22_write_proxies.json
  stage7/23_read_proxies.json
  stage8/28_hbench_v4_auto_frozen.json
  stage8/29_all_valid_covers.json
  stage8/30_hbench_v4_auto_summary.csv
  stage8/31_cross_query_routes.csv
  stage8/manifest.json
```

Para conferir o congelamento novamente sem recalcular os planos:

```bash
bash SchemaLens_V4_Auto_Prebenchmark/run_v4_auto.sh "$EXP" --verify-only
```

## Passo 3: comparação retrospectiva (opcional, não é benchmark novo)

A comparação abaixo lê resultados V4 já existentes **somente depois** de o novo HBench ter sido congelado. Ela fica em outra pasta, sem influenciar Stage 8:

```bash
ADDON="$(realpath SchemaLens_V4_Auto_Prebenchmark)"
EXP_ABS="$(realpath "$EXP")"
(cd "$EXP_ABS" && PYTHONPATH="$EXP_ABS:$ADDON" python3 -m v4_auto.retrospective --experiment "$EXP_ABS")
```

Saída: `outputs/v4_auto_posthoc/descriptive_existing_results.csv`. A análise é rotulada **POST_HOC_EXISTING_EXPLORATORY; NOT_INDEPENDENT_HOLDOUT**. O script não sobrescreve relatórios anteriores.

## Validade científica e limitações

- A seleção V4 utiliza **somente custos analíticos e estruturas físicas**. As latências anteriores da V3/V4 **não são importadas** pelo seletor.
- A seleção de fonte canônica é verificada em snapshots imutáveis. Atualizações atrasadas, exclusões, concorrência e manutenção da TS4 ainda não são validadas.
- A validação semântica é feita pelo avaliador lógico local e **não reproduz o SQL oficial do IoTDB**, nem a sequência/proporção operacional do IoT Benchmark.
- Os proxies de linhas são estimativas de execução; bytes lógicos não são bytes de SSTable e contagens de escrita não equivalem à vazão real.
- O resultado de pré-seleção não é ranking de p95. O módulo não executa Cassandra e não atribui ganho de latência a um deployment que não foi medido.
- Não gere novamente o HBench V3; não execute `python -m iot_pipeline.stage8` sobre a pasta original.
- A dimensão de 7 dias não foi confirmada por arquivos nesta etapa. Resultados de 1 e 30 dias são diagnósticos anteriores, não a avaliação independente do seletor automático.

## Exemplo conceitual

- Q2 justifica criar uma TS2-U para acessar dois sensores conjuntamente.
- Q7 não precisa ter ativado TS2 originalmente: uma TS2-U **já materializada** pode servir a agregação Q7, se o plano recompor os pontos e o resultado corresponder ao avaliador.
- Q8 pode usar a mesma TS2-U, lendo a partição em `timestamp DESC` até encontrar o último valor válido de cada sensor. A TS4 opcional só é favorecida quando o proxy representa vantagem adicional após considerar os custos de manutenção.
- TS2-D pode requerer acesso a vários buckets se uma consulta atravessar muitos dias. Esse número aparece explicitamente em `stage7/23_read_proxies.json`.

Assim, as ativações justificam **materialização**; a matriz cruzada justifica **reutilização**; e a seleção multicritério decide quais composições conservar para avaliação posterior.
