# Etapa 9 — Benchmark experimental Cassandra / TSBS (guia em português)

## Objetivo e posição na metodologia

As Etapas 3–8 já fixaram: 16 templates de consulta TSBS, 9 candidatos físicos de TS0/TS1/TS2/TS5/TS6, 37 QueryPlans, 9 WritePlans e 7 configurações selecionadas para o HBench (2 conservadoras, 5 condicionais). A Etapa 9 **não ativa famílias**, não cria regras para Cassandra genérico e **não reabre o ranking**. Seu trabalho é tentar materializar e executar *os deployments completos*, com um baseline físico externo equivalente à representação de séries escalares do Cassandra original do TSBS.

## Caminho dos dados

```text
TSBS --use-case cpu-only --format cassandra  →  arquivo texto ou .gz
                                               |
                                dataset.py: dez métricas completas
                                + series_id original + SHA-256 + tags
                                               |
                               instances.py: 16 templates × R repetições
                                               |
                 Stage 8 HBench fixado     +     baseline_series_double
                          |                              |
                Stage 6 WritePlans                   PK original
                          |                              |
                   Cassandra CQL               Cassandra CQL
                          \                              /
                           \--> Stage 5 client evaluator / 
                                          |
                                  oracle in-memory TS0
                                          |
                               igualdade de respostas?
                                  |                 |
                                 SIM                NÃO
                                  |                 |
                           medir latência         INVALID
```

O sistema mantém as dez métricas `usage_*` por `(hostname,timestamp_ns)` como evento lógico. Para o baseline, monta `series_id` segundo o serializador do TSBS:

```
cpu,hostname=host_0,region=...,...#usage_user#2020-01-01
```

A versão do TSBS usa `PRIMARY KEY(series_id,timestamp_ns)` em `series_double`. Nossa tabela `baseline_series_double` usa a mesma estrutura de chave, mas omite o `COMPACT STORAGE` histórico, para compatibilidade moderna. Isso significa **equivalência estrutural de chave**, não prova de desempenho igual à versão original do benchmark.

## Exemplo Q13 (high-cpu-1)

1. O TSBS produz dez leituras escalares por host/timestamp.
2. O WritePlan TS0 transforma esse evento em dez linhas, uma por métrica; o TS2 grava dez valores numa única linha temporal.
3. O QueryPlan escolhido para Q13 numa configuração TS0+TS2 usa `SELECT ... FROM ts2_cpu_wide_1d WHERE hostname=? AND bucket_day=? AND timestamp_ns >=? AND timestamp_ns <?`.
4. O operador lógico local filtra `usage_user > 90`; nenhum `ALLOW FILTERING` é usado.
5. O baseline recupera cada série de `baseline_series_double` por `(series_id, timestamp_ns)` e aplica o mesmo operador lógico.
6. Só medimos o tempo depois que as duas respostas coincidem com a referência TS0.

**Hipótese de pesquisa:** TS2 pode reduzir o número de requisições ao co-localizar métricas. O código não garante ganhos: p95, throughput e ocupação de disco devem ser medidos.

## Exemplo Q10 (double-groupby-1)

O QueryPlan TS6 lê registros de `(bucket_day,hour_ns)` com `hostname`, `sum_usage_user` e `count_usage_user`. A média deriva de `SUM / COUNT`, não de uma média simples entre médias. A consulta só pode usar diretamente os agregados por hora quando seus limites realmente estiverem alinhados. Se a instância tem `start=10:12:34` ou `end=22:12:34`, o runtime rejeita a rota TS6 e consulta TS0. Isso preserva a correção, mas é importante medir a **taxa de fallback** — uma configuração que raramente usa TS6 não deve receber crédito por ganhos imaginários de TS6.

Para experimentar uma condição favorável à TS6, utilize `--alignment-cohort aligned` e reporte em uma tabela separada da workload natural. Não ajuste os parâmetros depois de analisar as latências.

## O que acontece em uma execução live

1. Carrega os artefatos fixos das Etapas 4, 5, 6 e 8; verifica os hashes registrados no manifesto da Etapa 8.
2. Verifica `--data`, cardinalidade, 10 métricas, tags, timestamps e limite de observações em memória.
3. Gera as MESMAS instâncias de query para todas as configurações com a mesma seed.
4. Exige consentimento exato antes de recriar cada keyspace de teste.
5. Executa os CQLs da Etapa 4, além de criar a representação baseline.
6. Usa a implementação de referência da Etapa 6 para materializar as nove representações (somente as presentes no deployment) e escrever seus `INSERT`/`DELETE` via prepared statements.
7. Carrega o baseline a partir dos mesmos eventos lógicos, preservando `series_id`.
8. Verifica a leitura de uma amostra das linhas físicas no Cassandra.
9. Para cada instância, executa o plano escolhido ou o fallback TS0; compara com TS0 materializado em memória. Também valida o baseline.
10. Após a verificação, aquece e mede o tempo TOTAL do QueryPlan (CQLs + operações no cliente), armazenando mean/p50/p95/p99.
11. Salva resultados (33–36) em uma pasta **nova e separada**, nunca substituindo artefatos de seleção da Etapa 8.

**O que o tempo mede:** latência da função Python que invoca prepared CQL e executa composição/agrupamento no cliente. Não é um tempo interno do servidor, nem um número produzido pelo executável Go original. A taxa de ingestão neste release é da carga sequencial inicial, não a velocidade de escrita contínua sob concorrência.

## Como executar

Na pasta raiz do projeto acumulado:

```bash
python3 -m methodology.tsbs.stage9.cli --make-fixture inputs/stage9_dev_fixture.csv
python3 -m methodology.tsbs.stage9.cli --data inputs/stage9_dev_fixture.csv --all --dry-run
python3 -m unittest discover -s tests -v
```

O modo LIVE requer Cassandra acessível, driver opcional instalado e confirmação destrutiva explícita. Opcionalmente inicie o Cassandra local com `docker compose -f docker-compose.stage9.yml up -d` e aguarde o healthcheck (arquivo de infraestrutura fornecido, mas não testado neste ambiente):

```bash
python3 -m pip install -r requirements_stage9_optional.txt
python3 -m methodology.tsbs.stage9.cli \
  --data inputs/stage9_dev_fixture.csv --live \
  --deployment H_815d3cd1f0cb \
  --confirm-reset-keyspace sl_tsbs_h815d3cd1f0cb \
  --output outputs/tsbs_stage9_live_run1
```

Os comandos acima com fixture testam apenas o protocolo, usando dados **sintéticos**. Depois troque a origem por dados realmente gerados pelo TSBS, ajuste o intervalo de coleta da política de benchmark, faça warmups, repetições independentes e medições físicas de armazenamento.

## Limitações científicas que precisam aparecer no artigo

* **Sem avaliação real neste pacote:** a implementação foi testada offline; não havia Cassandra real neste ambiente.
* A instância de consulta é semântica e determinística, não o mesmo stream binário produzido pelo gerador Go do TSBS. O executor nativo TSBS (`tsbs_run_queries_cassandra`) deve ser avaliado separadamente se a alegação experimental exigir o baseline oficial literal.
* O baseline Cassandra é semelhante ao original pelo esquema de chave e formato de dados, mas não usa `COMPACT STORAGE`; não afirme que tempos deste adaptador são tempos publicados do TSBS.
* `ReferenceMaterializer` e o carregamento no Cassandra executam escritas sequenciais, não tolerantes a falhas, com toda a entrada em RAM. Não escale sem antes construir um writer incremental seguro e verificar idempotência após falhas.
* A amostra de linhas físicas não comprova integridade integral de todas as tabelas. Para resultados rigorosos, acrescente hashes completos de projeções e um oráculo independente dos operadores lógicos da Etapa 5.
* Latência p95 de três repetições não é estatisticamente robusta: a política inicial é um smoke test, não protocolo final. Aumente repetições e amostras com orçamento fixado previamente.
* A avaliação total requer observar p50/p95/p99, throughput, ingestão, amplificação de escrita, armazenamento real por SSTable (ex.: `nodetool tablestats`), consumo de CPU/memória e comportamento com dados atrasados.
* Não use resultados deste benchmark para ajustar regras TS0–TS8 ou escolher os cinco do HBench. A comparação retrospectiva com um conjunto expandido e válido é um experimento separado, necessário para medir regret/near-best.

## Relação com a próxima etapa

A Etapa 10 será responsável pela consolidação científica dos experimentos TSDB: repetição em escalas maiores, ablation TS0–TS8, cenários com e sem alinhamento, avaliação completa do custo de armazenamento e manutenção e comparação retrospectiva com espaço físico de referência. Não avance para conclusões de desempenho sem primeiro validar esta Etapa 9 em Cassandra real.
