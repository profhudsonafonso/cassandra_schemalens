# Etapa 8 — Consolidação de candidatos TSDB e HBench em Apache Cassandra

## 0. O que estamos implementando e o que **não** foi medido

Este projeto é uma **adaptação acadêmica** inspirada no TPCx-IoT 2.1.2; o SQL lógico atual presume um filtro por `sensor_name` (sem exigir `driver_system_id`) e retorna leituras em duas janelas fechadas de 5 segundos. **O código real do kit TPCx-IoT continua sem verificação**. Nada nesta etapa atesta conformidade TPC, executa um cluster Cassandra ou fornece p95, IoTps, resultados de disco real, Top-k preservation ou regret.

Em termos do artigo original SchemaLens/DKE, esta etapa operacionaliza a consolidação **pré-benchmark** de candidatos das Seções 3.6–3.7 (Algoritmos 4 e 5): equivalência física, cobertura validada, compatibilidade, composições completas, pré-seleção Pareto e geração de HBench. O artigo exige que resultados medidos provenham de configurações completas implantadas conjuntamente; isso é reservado à etapa 9.

## 1. Linha do tempo: contratos imutáveis que a etapa 8 recebe

1. **Etapas 1–3** — EER, seleção de raízes, matriz, ativação de `TS0`, `TS1`, `TS3`, `M1`, `M3`, sem medição ou ajuste por desempenho.
2. **Etapa 4** — cinco especificações físicas distintas: `ts0_series_raw`, `ts1_series_60s`, `ts1_series_3600s`, `ts3_sensor_60s`, `ts3_sensor_3600s`.
3. **Etapa 5** — cinco QueryPlans e verificações sintéticas para leitura de duas janelas; a cobertura é `CONDITIONAL_OFFLINE_ONLY`.
4. **Etapa 6** — cinco WritePlans e verificações sintéticas de repetição/falha parcial; não há atomicidade distribuída nem reparo durável comprovados.
5. **Etapa 7** — estimativas de partições, quantidade de CQL, armazenamento lógico replicado e instruções INSERT para **um cenário sintético**. Alertas de partição são diagnósticos, não exclusão categórica.
6. **Etapa 8** — compor candidatos em deploys completos **condicionais**, preservar referências de QueryPlan e WritePlan e produzir HBench limitado a 5 configurações.

O programa `consolidator.py` valida os hashes de entrada anteriores, recomputa as estimativas JSON da etapa 7 para conferir a origem e recusa qualquer política que solicite informação medida ou declare cobertura já validada em Cassandra.

## 2. Canonicalização — Algoritmo 4

A assinatura `signature = sha256(canonical_json(layout))` inclui tipos de colunas, PK, CK, ordenação, bucket, predicado de acesso, templates CQL e requisitos de manutenção. Exclui **somente** o nome textual da tabela para que outra tabela de desenho fisicamente idêntico não seja tratada como alternativa distinta. A deduplicação agrupa assinaturas iguais; neste experimento **as cinco são diferentes**. Mesma família ou mesmo nó conceitual `Reading` não autoriza fusão.

`50_canonical_shared_candidates.json` registra origem das especificações e escopo de materialização. `51_validated_conditional_coverage.json` registra separadamente **origem** de ativação e **cobertura possível**: `TS1` não se originou da consulta de ingestão, mas dispõe de WritePlan para inserir os mesmos eventos. A cobertura é validada somente por testes offline, não pelo Cassandra.

## 3. Compatibilidade e dependências — Algoritmo 4

Uma configuração completa neste perfil deve conter:

- **Exatamente uma tabela canônica** (`TS0`, `TS1/60`, ou `TS1/3600`), para definir um único caminho autoritativo de escrita no deployment estudado;
- **Zero ou uma projeção TS3** (`TS3/60` ou `TS3/3600`), como segundo acesso materializado;
- QueryPlan que possa recuperar a consulta `Q_READ_2_WINDOWS` de uma tabela selecionada; e um WritePlan para **cada** tabela materializada, cobrindo `Q_INGEST`.

Uma `TS3` sozinha não é admissível; requer uma fonte canônica de eventos. Combinações com duas fontes canônicas ou duas projeções TS3 são intencionalmente excluídas pela política experimental predeclarada: esta primeira rodada testa uma fonte + no máximo uma projeção. Esses desenhos **não são teoricamente impossíveis**; ficam fora do catálogo de deploys completos deste protocolo controlado. Alterar a política para comportar outras combinações exige versionamento de protocolo, antes de olhar resultados.

Cálculo exato: para 5 candidatos há `2^5 - 1 = 31` subconjuntos não vazios. Desses, **9** respeitam as regras anteriores (3 canônicos × 3 escolhas de projeção: nenhuma, TS3/60 ou TS3/3600). Os **22** descartes são registrados em `59_excluded_combinations.json`, com razão específica. Não há truncamento oculto.

Exemplo de deployment completo: `TS1/3600 + TS3/3600`.

- Um `INSERT` no canônico TS1/3600 e outro no TS3/3600 por evento de origem (dois inserts lógicos).
- `Q_READ_2_WINDOWS` escolhe o QueryPlan TS3/3600 porque a estimativa sintética traz 3 chamadas CQL, contra 30 chamadas via TS1/3600 para o mesmo exemplo.
- Essa escolha por fan-out **não promete menor latência** nem garante equivalência operacional: os dois caminhos precisam ser executados e testados ao vivo na etapa 9.
- Se o segundo INSERT falhar, a configuração ficará transitoriamente inconsistente; precisamos de fonte durável/replay/reparo de projeção antes de afirmar correção operacional.

## 4. Critérios de Pareto e pré-seleção — Algoritmo 5

Os custos de um deployment **somam estruturas físicas**, nunca cobram apenas a rota de leitura mais curta deixando uma projeção sem custo:

- `read_cql_requests`: número de chamadas CQL no plano selecionado (menor fan-out, desempate estável por ID);
- `replicated_logical_storage`: soma dos bytes lógicos replicados **estimados**, incluindo duplicação;
- `insert_statements_per_event`: número de tabelas que receberão escrita por evento;
- `maximum_partition_bytes`: maior partição esperada **entre todas** as tabelas selecionadas;
- `physical_partition_count`: soma das partições previstas entre todas as tabelas.

**São proxies diferentes, não milissegundos, bytes reais de SSTables ou amplificação interna do Cassandra.** Uma configuração A domina B quando seus cinco proxies são <= e pelo menos um é <. Com as premissas sintéticas atuais: **9 composições condicionais → 7 não dominadas**. As duas dominadas também são registradas.

A política `config/tpcx_stage8_consolidation_policy.json` pré-registra:

- `top_k = 5` e enumeração exata até 12 candidatos (a entrada real tem somente 5);
- normalização dos proxies pelas medidas analíticas de `TS0` isolada (não pelo vencedor observado);
- pesos apenas **didáticos**: 45% chamadas de leitura, 15% armazenamento, 20% instruções de escrita, 15% tamanho máximo de partição e 5% número de partições;
- seleção restrita à frente de Pareto com controle TS0 obrigatório e diversidade gulosa de **variantes físicas distintas**, antes de desempatar pelo custo normalizado.

O score de proxies não tem unidade física e **não é performance estimada**. Essa seleção não infere a melhor configuração; é uma estratégia transparente de cobertura de alternativas para benchmark. Como a diversidade é priorizada, uma candidata com score proxy alto pode ser retida se explorar uma alternativa de bucket que não aparece nas demais.

## 5. Resultado efetivo HBench, e sua interpretação

| Ordem de pré-seleção | Tabelas | Rota de leitura | CQL por leitura | INSERTs/evento |
|---|---|---|---:|---:|
| H1 | TS0 | TS0 | 20 | 1 |
| H2 | TS1/3600 + TS3/3600 | TS3/3600 | 3 | 2 |
| H3 | TS1/60 + TS3/60 | TS3/60 | 3 | 2 |
| H4 | TS0 + TS3/3600 | TS3/3600 | 3 | 2 |
| H5 | TS1/3600 | TS1/3600 | 30 | 1 |

A ordem **não é** ranking de desempenho. A alternativa H3 tem, sob as premissas da etapa 7, custo estrutural alto (muitas partições pequenas). Ela foi preservada **como controle experimental da granularidade menor**, não como escolha otimizada.

A redução dentro das **nove composições condicionais admissíveis** é `1 - 5/9 = 44,44%`. Não devemos comparar 5 contra 31 alegando “83,87% de redução entre designs completos”: os outros 22 subconjuntos não cumprem o escopo da política, e não são uma baseline equivalente. Também **não** chamaremos isso Top-k preservation, regret ou near-best sem o oráculo pós-medição.

**Restrição de inferência:** esses números refletem somente 2 consultas lógicas (`Q_INGEST`, `Q_READ_2_WINDOWS`) e 5 representações geradas. Isso não demonstra generalização a outras consultas time-series do TSBS nem a uma eventual interpretação agregada do kit oficial.

## 6. Arquivos de saída e proveniência

Todos são escritos em `outputs/tpcx_iot_stage8/`:

| Arquivo | Propósito |
|---|---|
| `50_canonical_shared_candidates.json` | Identidade física canônica e origem |
| `51_validated_conditional_coverage.json` | QueryPlan, WritePlan, guardas e cobertura condicional |
| `52_complete_compatible_deployments.json` | 9 composições com dependências e rotas |
| `53_pareto_preselection.json` | 7 alternativas Pareto, 2 dominadas, eixos de custo |
| `54_hbench_selected.json` | 5 HBench e todas suas tabelas |
| `55_hbench_query_routes.json` | Roteamento para ambas as operações |
| `56_hbench_summary.csv` | Tabela de configuração e proxies |
| `57_stage9_execution_plan.csv` | Caminhos de CQL DDL e WritePlans para etapa 9; nada executado |
| `58_stage8_manifest.json` | Contagens, hashes e guardas metodológicas |
| `59_excluded_combinations.json` | Justificativas para 22 exclusões incompatíveis + 2 dominâncias |

As saídas contêm explicitamente `live_cassandra_executed: false`, `operational_coverage_proven: false`, `selection_uses_measured_results: false`. Rodar os scripts **não** altera os artefatos anteriores.

## 7. Execução pelo terminal do VS Code

Requer Python 3.10+; somente biblioteca padrão:

```bash
cd SchemaLens_TPCx_IoT_Cassandra_Stage8
python3 -m methodology.tpcx_iot.stage8.cli
python3 -m examples.show_stage8_hbench
python3 -m unittest discover -s tests -v
# alternativamente:
./run_stage8.sh
```

## 8. Etapa 9 — Experimentos que faltam

1. **Confirmar o código do kit TPCx-IoT**, predicados reais, semântica de retorno/agregação, seleção de sensores e janelas. Se não coincidir com o SQL congelado, versionar um novo perfil e rerodar as etapas analíticas **antes** dos experimentos. Nunca alterar regras depois de ver o vencedor.
2. **Instanciar cada HBench integralmente** em keyspace isolado no Cassandra com configurações idênticas de replicação, drivers, hardware e aquecimento.
3. **Implementar registro durável de eventos, projeções e recuperação**, com idempotência revalidada sob falha parcial e reinicialização.
4. Validar exaustivamente equivalência de leitura em janelas, fronteiras de buckets, eventos com timestamps iguais, catálogo de estações completo e leitura durante ingestão.
5. **Medir somente após correção**: throughput de ingestão, latência p50/p95/p99, armazenamento real, taxas de erro, tamanho das partições, CPU, memória e I/O.
6. Comparar HBench às outras composições **após** finalizar a seleção e executar as medidas: redução de número de deployments, Pareto medido, perda (regret) por objetivo e preservação de alternativas competitivas. Evitar equiparar o benchmark adaptado a resultado oficial TPC.

A preseleção de etapa 8 é um resultado **analítico condicionado**, e não uma conclusão de performance.
