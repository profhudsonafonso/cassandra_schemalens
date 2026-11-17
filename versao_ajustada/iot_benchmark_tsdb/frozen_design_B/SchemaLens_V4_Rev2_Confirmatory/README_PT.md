# SchemaLens IoT Cassandra — V4 Rev2 e avaliação confirmatória

**Escopo:** melhorar apenas a estimativa analítica do Stage 7 e a seleção explicável do Stage 8, preservando o HBench V3 original e o HBench V4 Rev1. Depois criar **um novo dataset pelo gerador Java original**, congelar um novo HBench Rev2 e somente então medir no Cassandra.

**Status de entrega:** código desenvolvido e testado offline (11 testes unitários + teste de congelamento integrado com fixture). **Ainda não existem novas medições Cassandra da avaliação confirmatória.** O gerador Java requer Git, Maven e JDK no Ubuntu. A implementação confirmatória não é o cliente SQL oficial do IoT Benchmark: usa 12 objetos de parâmetros do Java e uma tradução Cassandra auditável.

## Etapa A — como Stage 7 e Stage 8 mudaram

Stage 4, Stage 5 e Stage 6 continuam usando as candidatas físicas e o avaliador lógico do V4 Auto Rev1. A nova lógica acrescenta:

- **Stage 7:** `request_count_proxy` (chamadas CQL), `physical_rows_proxy`, `returned_bytes_proxy`, `bucket_count_proxy`, `extra_bucket_fanout_proxy`, `max_partition_bytes_proxy` e `partition_pressure_proxy`.
- **Buckets de calendário vazios:** para consultas com intervalo explícito, TS2-D contabiliza até buckets sem dados, pois o executor emite essas consultas CQL. Q5 sem intervalo usa os dias conhecidos.
- **Partições:** tamanho máximo estimado, limites preventivos 32 MiB (soft) e 128 MiB (hard), calculados por estrutura, antes de qualquer medição. Esses limites são **hipóteses de engenharia**, não limiares calibrados empiricamente nem garantias de operação do Cassandra.
- **Stage 8:** usa os novos custos para selecionar rotas, enumera alternativas canônicas TS0/TS2-U/TS2-D com TS4 opcional, Pareto multicritério e salvaguarda de diversidade. Cada rota selecionada tem um registro explicável de features e custo. Os IDs usam prefixo `R2H_`, para não confundir com `V4H_` da Rev1.
- **Sem ajuste retrospectivo:** todos os coeficientes são declarados em `v4_rev2/cost.py` antes das medições confirmatórias. Não há leitura de métricas do Stage 9/diagnósticos para seleção.

Forma do read proxy (unidades abstratas de custo):

```text
C_read = 4.0 * CQL_requests
       + 0.02 * physical_rows
       + 0.20 * returned_KiB
       + 2.0 * extra_bucket_coordination
       + 5.0 * partition_pressure
```

O número de requisições cobre custo de ida/volta; a penalidade de fan-out busca representar *coordenação entre buckets acima das requisições dos operandos lógicos*. Trata-se de custos distintos modelados, não de mensuração ou regressão de latência. Bytes de resposta/partição são **proxies**, não medições de rede ou SSTables. Coeficientes não foram ajustados para favorecer o resultado anterior TS2-U. Em um dataset de somente um dia, TS2-U e TS2-D ainda podem empatar legitimamente.

## Etapa B — validar a Rev2 usando os dados HISTÓRICOS (somente exploratório)

Na raiz `SchemaLens_IoT_Cassandra_V3_Automatico`, coloque o ZIP e extraia:

```bash
unzip -o SchemaLens_V4_Rev2_Confirmatory.zip
ADDON="$(realpath SchemaLens_V4_Rev2_Confirmatory)"
EXP_OLD="$(realpath native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748)"

bash "$ADDON/run_tests.sh"
bash "$ADDON/run_rev2.sh" "$EXP_OLD"
bash "$ADDON/run_rev2.sh" "$EXP_OLD" --verify-only
```

Saída **nova** em `"$EXP_OLD/outputs/v4_rev2_prebenchmark"`, **nunca** em `outputs/v4_auto_prebenchmark` Rev1. Esse primeiro congelamento serve para testar software/custos; não constitui confirmação independente. É proibido usar a latência antiga para modificar suas escolhas depois de congelar.

Novos arquivos-chave:

```text
outputs/v4_rev2_prebenchmark/
  stage7/23_read_proxies_rev2.json
  stage7/24_partition_proxies_rev2.json
  stage7/25_rev2_cost_policy.json
  stage8/28_hbench_v4_rev2_frozen.json
  stage8/29_all_compatible_covers.json
  stage8/30_hbench_v4_rev2_summary.csv
  stage8/31_cross_query_routes.csv
  stage8/32_selection_explanations_rev2.json
  stage8/manifest.json
```

## Etapa C — criar geração Java INDEPENDENTE (antes do novo HBench)

Execute no **mesmo repositório raiz**. O programa faz uma cópia independente do código, muda os seeds `DATA_SEED` e `QUERY_SEED` na cópia, usa o `auto_official_v3.py` com o upstream pinado `62e0dc100e56443978c8b4c66472b1d8a7ef0238`, gera dados CSV via **generateDataMode** e captura parâmetros Java. Ele não manipula os dados V3, não usa replicação sintética de dias e não executa Cassandra.

```bash
PROJECT="$(pwd)"
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"

PYTHONPATH="$ADDON" python3 -m v4_rev2.prepare_holdout \
  --source-project "$PROJECT" \
  --reference-v3 "$EXP_OLD" \
  --workspace "$WORK" \
  --devices 6 --loops 300 --batch-size 5 \
  --data-seed 19243 --query-seed 38421 \
  --point-step-ms 600000 \
  --query-interval-ms 86400000 \
  --group-by-ms 3600000 \
  --max-cells 100000
```

A proposta inicial estima **18.000 células métricas** (6 dispositivos × 2 sensores × 300 loops × batch 5). `POINT_STEP=600000` busca gerar uma série atravessando dias, sem copiar artificialmente linhas. A duração efetiva deverá ser verificada nos CSVs Java. A geração pode levar o tempo de build Maven; não há execução da etapa confirmatória sem sucesso de seu gerador.

**Atenção:** o script exige workspace inexistente e fora da raiz do projeto original. Se a geração falhar, preserva o workspace e o log para diagnóstico — não apague o HBench V3. Uma nova tentativa deve usar outro workspace. A cópia Java mantém **dois sensores DOUBLE alinhados**; ampliar para outros tipos/número de sensores exigirá revisão separada.

Para simular o plano **sem gerar nada** adicione `--dry-run` ao comando anterior.

Ao concluir, haverá um único experimento novo, que pode ser localizado por:

```bash
NEW_EXP="$(find "$WORK/java_work/runs" -maxdepth 2 -type d -name schemalens -print -quit)"
test -f "$NEW_EXP/outputs/stage8/32_frozen_manifest.json"
echo "$NEW_EXP"
```

Um alias de captura `native_queries_q12fixed_confirm_rev2.jsonl` é criado **byte a byte** a partir do próprio `native_queries.jsonl`. Não inventa novas consultas; existe apenas para cumprir a verificação de proveniência do seletor Rev1.

## Etapa D — CONGELAR HBench Rev2 no holdout, sem medir

```bash
bash "$ADDON/run_rev2.sh" "$NEW_EXP" \
  --confirmatory --reference-v3 "$EXP_OLD"

bash "$ADDON/run_rev2.sh" "$NEW_EXP" --verify-only
```

**Portas de validação:** a ferramenta confere a seleção V3 da geração nova e seus hashes; compara os SHA dos datasets e objetos de parâmetros Java com o V3 histórico; valida a origem dos 12 tipos de operação no JSONL Java; recusa usar a mesma carga ou parâmetros da V3; audita equivalência lógica; calcula Stage7+Stage8; registra hashes do código e da seleção. Se qualquer falha ocorrer, **não execute a medição**. Não altere pesos após congelar.

## Etapa E — benchmark confirmatório Cassandra (SOMENTE após D)

O executor só aceita as configurações que constarem no **HBench Rev2 congelado**, mais duas leituras da mesma tabela de referência: baseline original e Q8 otimizada. Ao iniciar, valida resultados contra o oracle para as 12 consultas antes da primeira medição. Os únicos keyspaces recriados têm prefixo `sl_conf_r2_<hash>_`, identificado pelo hash da seleção. **Somente execute em Cassandra descartável e confira que nenhum desses keyspaces contenha dados valiosos.**

```bash
bash "$ADDON/run_confirmatory.sh" "$NEW_EXP" \
  --host 127.0.0.1 --port 9043 \
  --rounds 40 --warmup 5 --page-size 16 \
  --measurement-seed 80421 \
  --max-cells 100000 \
  --confirm-reset RESET_ONLY_SCHEMA_CONF_R2_KEYSPACES
```

Saída: `"$NEW_EXP/outputs/v4_rev2_confirmatory/batch_<SHA8>/"` com `raw_latencies.csv`, `query_p95.csv`, `aggregated.csv`, `orders.csv`, `load.csv` e `manifest.json`. Recusa sobrescrever resultados existentes. `aggregated.csv` apresenta **média dos p95 por classe**, com e sem Q8. Não é p95 do workload misto real.

Para envio:

```bash
cd "$NEW_EXP"
zip -r "$PROJECT/V4_Rev2_Confirmatory_results.zip" \
  outputs/v4_rev2_prebenchmark \
  outputs/v4_rev2_confirmatory
```

## O que examinar após o experimento

1. Quantas configurações completas o Stage 8 Rev2 preservou; por quais razões (Pareto/diversidade), e se TS2-U / TS2-D continuam empatadas ou a evidência temporal as separa.
2. Diferenças entre proxies de requisições/buckets estimados e contagens reais do executor Cassandra, especialmente Q2, Q5, Q8 e Q12.
3. p95 de cada consulta e média com/sem Q8 contra baseline otimizada; não declarar vitória por um ganho inferior à variabilidade experimental.
4. Custo de escrita, bytes reais de SSTables (ainda não instrumentados), consistência com updates e novas escalas. TS4 aqui é somente snapshot offline; NÃO comprova manutenção com escritas concorrentes.

## Limites científicos (não omitir no artigo)

- Esta é uma **avaliação confirmatória do seletor Rev2** com dados novos, **não uma reprodução completa do escore IoT Benchmark oficial**.
- O gerador é o Apache IoT Benchmark Java; porém a captura contém **um objeto de parâmetros por classe**. Não há reprodução do SQL IoTDB, da distribuição operacional `OPERATION_PROPORTION` nem de sua frequência/ordem real.
- Os resultados V3 e V4 Rev1 motivaram a hipótese e a escolha de features Rev2; portanto *a avaliação independente é somente a nova amostra Java*, não a seleção ou a escolha dessas features antes das campanhas antigas.
- Coeficientes são prospectivos, pré-registrados, não validados estatisticamente. O teste é diagnóstico e não substitui avaliação com dados/consultas de múltiplos workloads ou repetições de campanha independentes.
- Q8 com valores nulos e Q12 com operandos distintos são verificadas contra o oracle local, não contra o executor IoTDB oficial.
- Com apenas dois sensores, a generalização a sensores esparsos, mais colunas e maior concorrência não está comprovada.
