# SchemaLens Cassandra — Rev3: avaliação independente com novas gerações Java

## Objetivo e rigor experimental

Esta extensão **não** modifica Rev2, Rev3 ou HBench anteriores. Deriva a Rev3 de duas **novas gerações** de dados + objetos Java, usando **exatamente o código Rev3 auditado**. Congela a seleção e o protocolo de medição antes de conectar ao Cassandra. O benchmark confirma correção semântica e mede desempenho e operações das alternativas selecionadas + duas baselines da mesma tabela de referência. Não é replay do escore oficial IoT Benchmark.

### Confirmação do `rev3_frozen_complete.tar.gz` enviado

Os oito artefatos do manifesto foram verificados byte a byte por SHA-256 (8/8), além dos três arquivos-fonte `core.py`, `prebenchmark.py` e `runtime.py` (3/3). A política contém os coeficientes da Rev2 e explicita que são *proxies*, não tempos calibrados. O estado do HBench histórico Rev3 é **exploratório pós-E5**. As novas campanhas testam a metodologia já definida, não tornam retroativamente o estudo histórico independente.

### Duas campanhas registradas antes de medir

| Configuração | Campanha A | Campanha B |
|---|---:|---:|
| DATA_SEED | `531007` | `531101` |
| QUERY_SEED | `531013` | `531107` |
| Dispositivos | 6 | 9 |
| Loops por dispositivo | 300 | 600 |
| Leituras por lote | 5 | 5 |
| Sensores DOUBLE | 2 | 2 |
| Células métricas esperadas | 18.000 | 54.000 |
| `POINT_STEP` | 600.000 ms | 600.000 ms |
| `QUERY_INTERVAL` | 86.400.000 ms (1 dia) | 604.800.000 ms (7 dias) |
| `GROUP_BY_TIME_UNIT` | 3.600.000 ms | 3.600.000 ms |
| Execuções cronometradas por Q/variante | 60 | 60 |
| Warmups | 5 | 5 |

**Atenção:** as quantidades são a configuração solicitada ao gerador Java, e a abrangência temporal real será conhecida apenas quando os CSVs forem produzidos. Os dois seeds são independentes dos históricos anteriores. A diversidade de dados **não** equivale a replicar distribuição oficial de operações, variações de tipo ou sensores esparsos: o gerador atual captura **um objeto por Q1–Q12** e dois sensores DOUBLE alinhados.

## Requisitos

1. Ambiente virtual Python com `cassandra-driver`; Java, Maven, Git, Docker e o Cassandra local do projeto.
2. Na raiz do projeto já devem existir as pastas `SchemaLens_V4_Rev2_Confirmatory` e `SchemaLens_V4_Rev3_PlanAware`, além dos históricos V3 e Rev2 indicados abaixo.
3. Executar em **Cassandra de pesquisa descartável**. O runner **nunca faz DROP nem TRUNCATE**, mas cria novos keyspaces `sl_conf_r3_*` e insere dados neles; não use em servidor compartilhado ou de produção.
4. Os scripts exigem `source` e experimentos históricos **somente para leitura**. Se seus caminhos forem diferentes, exporte `REFERENCE_V3` e `REFERENCE_REV2` antes dos comandos.

## Instalação

Na raiz `~/Documents/schemalens_cassandra/SchemaLens_IoT_Cassandra_V3_Automatico`:

```bash
source ~/Documents/schemalens_cassandra/venv/bin/activate
unzip -o SchemaLens_Rev3_Independent_Confirmatory.zip
PACK="$(realpath SchemaLens_Rev3_Independent_Confirmatory)"
bash "$PACK/run_tests.sh"
```

## Passo 1 — campanha A: gerar dados NOVOS, sem Cassandra

```bash
bash "$PACK/run_campaign.sh" A generate
```

O gerador Java pode levar um tempo e cria um workspace novo **fora da raiz original**, em `../schemalens_rev3_new_java_A_20261105`. Em caso de falha, o workspace permanece como evidência; não o apague para tentar novamente. Se precisar de nova tentativa, use `WORKSPACE_OVERRIDE` com outro caminho inexistente.

## Passo 2 — congelar antes de medir

```bash
bash "$PACK/run_campaign.sh" A freeze
bash "$PACK/run_campaign.sh" A preflight
```

O script executa o Stage 7/8 da Rev2 como base de custos físicos e, sobre a seleção desse novo dataset, os Stages 5/7/8 da Rev3. O resultado é uma **nova Rev3** (IDs `R3H_*`) derivada sem p95. Registra `REV3_CONFIRMATORY_PREREGISTRATION.json` com hashes de dados/consultas, seleção, código, seeds, referências, 60 medições, 5 warmups e execução balanceada. A validação da Rev3 exigirá os mesmos `plan_id` no Stage 9.

O estado interno da Rev3 preserva a etiqueta histórica `FROZEN_REV3_EXPLORATORY_PLAN_AWARE`, porque não modificamos o módulo original. A **nova independência** é registrada no manifesto adicional gerado antes do benchmark. Não altere esses arquivos após congelar.

## Passo 3 — executar no Cassandra descartável

Confira o contêiner e a porta:

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}'
```

Após confirmar que `127.0.0.1:9043` é sua instância experimental:

```bash
bash "$PACK/run_campaign.sh" A benchmark
```

Este comando tem confirmação explícita para **CREATE ONLY** embutida no shell. O Python valida hashes **antes** de se conectar, confere que TODOS os keyspaces calculados não existem antes do primeiro CREATE e então cria tabelas e insere dados somente nesses namespaces. **Nenhuma tabela existente é resetada.** Uma falha de materialização ou semântica interrompe a campanha sem sobrepor resultados; os keyspaces já criados permanecem para investigação.

A baseline `BASELINE_ORIGINAL` e `BASELINE_Q8_OPTIMIZED` são duas leituras da **mesma** tabela; os dados são carregados uma única vez.

Cada consulta é validada contra seu oráculo antes de medir, recebe warmup por variante, e as variantes são executadas em ordem pseudoaleatória e alternada. O resultado é p95 por consulta e média **não ponderada** dos p95 das 12 classes, não p95 de um workload com frequências reais.

Arquivos: `raw_latencies.csv`, `orders.csv`, `query_p95.csv`, `aggregated.csv`, `load.csv`, `stage7_vs_stage9.csv`, `manifest.json`. Após o flush manual, também é gerado o snapshot de armazenamento físico.

## Medir armazenamento físico após o benchmark (recomendado)

**Somente depois que as latências da campanha A estiverem concluídas**, imprima os comandos de flush específicos dos novos keyspaces; o programa **não os executa**:

```bash
bash "$PACK/run_campaign.sh" A show-flush
```

Revise os nomes exibidos e, caso o Cassandra seja exclusivamente de teste, execute manualmente essas quatro instruções `docker exec ... nodetool flush KEYSPACE`. Depois faça o snapshot de SSTables:

```bash
bash "$PACK/run_campaign.sh" A storage
```

A coleta executa apenas `nodetool tablestats` e não modifica nenhum banco. Ela escreve `outputs/rev3_postbenchmark_storage/table_storage.csv`, `deployment_storage.csv` e os arquivos brutos. Se as SSTables aparecerem como zero, a medição física não está pronta; **não interprete zero como custo real de disco**.

Para B, repita com `B show-flush` e `B storage` após `B benchmark`.

## Passo 4 — exportar os resultados A

```bash
bash "$PACK/run_campaign.sh" A pack
```

Encontra em `SchemaLens_IoT_Cassandra_V3_Automatico/rev3_confirmatory_A_results.zip`. Inclui os resultados, seleção e políticas congeladas, hashes de geração, CSV de dados do Java e parâmetros das 12 consultas. **Envie esse ZIP** para auditoria. Se a campanha falhar, **não utilize `pack`**; envie o `failure_manifest.json` e log do terminal.

## Passo 5 — repetir B sem modificar A

```bash
bash "$PACK/run_campaign.sh" B generate
bash "$PACK/run_campaign.sh" B freeze
bash "$PACK/run_campaign.sh" B preflight
bash "$PACK/run_campaign.sh" B benchmark
bash "$PACK/run_campaign.sh" B pack
```

A campanha B usa outros seeds, mais dispositivos e histórico, e janela de 7 dias. Não altere a política de custo após os resultados A. Os experimentos têm workspaces, hashes e namespaces Cassandra diferentes. O segundo arquivo será `rev3_confirmatory_B_results.zip`.

## O que exatamente iremos avaliar

- **Correção semântica** em todas as combinações Q/variantes, antes e durante medições.
- **Desempenho**: p95 por Q/variante e média de p95 com/sem Q8 ou Q5, comparada primordialmente à baseline Q8 otimizada.
- **Fidelidade do custo**: `request_count_proxy` e `physical_rows_proxy` versus chamadas e linhas iteradas pelo executor; diferenças são registradas, não corrigidas retrospectivamente.
- **Custos de escrita**: operações do loader Python por implantação, não throughput real de ingestão.
- **Efeito do histórico e bucket**: comparar TS2-U e TS2-D à medida que A/B mudam histórico e janela.
- **Armazenamento em disco**: coletado em etapa separada após o benchmark e flush manual, nunca durante a medição de latências.

## Proibições e limitações

- Nunca reusar resultados Rev2/E5 para calibrar o HBench Rev3 desta geração.
- Não declarar p95 de 12 consultas como *p95 do workload real*.
- Não atribuir `execute_calls` ou linhas iteradas ao trabalho interno do Cassandra.
- Não inferir vantagem de TS2-D em escala maior sem medições; dados mais longos podem aumentar fan-out e custos.
- Não realizar `DROP`, `TRUNCATE`, `FLUSH` automático nem mudar configurações do cluster durante a campanha.
- Não selecionar outro plano após observar p95. Para uma futura Rev4, exigir novas campanhas independentes.
- 60 repetições por Q são uma medição inicial, mas não substituem repetição entre dias/máquinas/campanhas; comparar intervalos e outliers.
