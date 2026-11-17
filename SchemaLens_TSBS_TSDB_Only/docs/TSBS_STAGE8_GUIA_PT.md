# Etapa 8 — Consolidação completa da workload TSBS e HBench pré-experimento

## 1. O problema que estamos resolvendo

Até a Etapa 7, temos 16 consultas TSBS `cpu-only`, 9 candidatos físicos Cassandra, 37 alternativas de QueryPlan, um WritePlan por candidato e proxies analíticos de custos. A Etapa 8 responde a outra pergunta: **quais conjuntos de tabelas poderiam sustentar *toda* a workload ao mesmo tempo?**

Não selecionamos uma tabela vencedora isoladamente por consulta. A configuração final tem de integrar as representações compartilhadas e respeitar a necessidade de alimentar/atualizar todas as tabelas escolhidas. Seguimos a abordagem de **consolidação tardia** (após a geração de candidatos por consulta).

## 2. Exemplo mínimo

- Q01 (`single-groupby`) possui planos TS0 e TS5 (MAX por minuto).
- Q10 (`double-groupby`) possui planos TS0, TS5 (SUM/COUNT) e TS6 (por hora e host).
- Q13 (`high-cpu-1`) possui TS0, duas variantes TS1 e TS2 (métricas co-localizadas).
- Q15 (`lastpoint`) possui apenas TS0, portanto **qualquer deployment completo atual deve incluir TS0**.

Por exemplo, um deployment `[ts0_metric_raw, ts2_wide_bucket_1d]` possui duas tabelas, mas serve **16/16 templates estruturalmente**: lê quase todas via TS0 e pode usar TS2 para Q13. O custo estrutural do TS2 é adicional: a TS0 continua presente, com seu respectivo armazenamento e escrita.

Já `[ts0_metric_raw, ts6_hour_host_dimension]` pode usar TS6 para Q10–Q12, **apenas se** as janelas forem adequadamente alinhadas; caso contrário os planos TS0 permanecem a alternativa correta. Além disso, a atualização da TS6 depende de fonte canônica persistente (Etapa 6 implementou uma versão *in-memory* de referência, não um serviço de produção).

## 3. Passo a passo do algoritmo

1. **Carrega e valida** os arquivos originais das Etapas 4–7. Não reexecuta TS0–TS8 nem C0–C9. Confere unicidade, relações entre consulta↔candidato↔QueryPlan↔WritePlan, cobertura TS0 e checksums dos manifests anteriores.
2. **Enumera cada subconjunto** das nove tabelas contendo `ts0_metric_raw`, com limite predefinido que **gera erro**, jamais truncamento silencioso. Como TS0 é obrigatória, há `2^(9−1)=256` subconjuntos por perfil se `max_tables_per_deployment=9`.
3. **Para cada consulta**, escolhe dentro do subconjunto o QueryPlan de menor **proxy analítico normalizado** (desempate por `plan_id`). No perfil conservador, o plano com `alignment_guard` é excluído. No condicional, esses planos podem ser escolhidos, mas suas pré-condições aparecem explicitamente na rota da consulta.
4. **Exige cobertura estrutural 16/16** e todos os WritePlans. Elimina subconjuntos que incluem tabelas sem nenhum QueryPlan selecionado (canonicalização). A fonte TS0 permanece mesmo quando não tiver sido a rota mais barata de alguma consulta, devido à dependência dos rollups e a Q15.
5. **Calcula três componentes separados**: leitura média dos templates (uniforme **assumida**), armazenamento agregado da configuração, e total de chaves físicas afetadas por evento de escrita. Normaliza armazenamento/escrita pelo deployment somente TS0.
6. **Filtra por Pareto**: um deployment domina outro quando não piora em nenhum dos três proxies e melhora estritamente em ao menos um. Os avisos de risco permanecem exibidos, mas **não** são convertidos artificialmente em inviabilidade ou pesos sem evidência.
7. **Ordena a frente não dominada** por pesos definidos na política e retém até `top_k=5` por perfil. O ranking é somente para escolher configurações a testar no HBench futuro.

## 4. Fórmulas (todos os números são proxies, NÃO milissegundos)

Para consulta `q` e plano `p`:

\[
R_{q,p}=0.2\frac{\mathrm{Requests}_{q,p}}{\mathrm{Requests}_{q,TS0}} +
  0.8\frac{\mathrm{Rows}_{q,p}}{\mathrm{Rows}_{q,TS0}}.
\]

Cada consulta tem o mesmo peso **hipotético** `1/16` porque as frequências reais não foram fornecidas:

\[
R_H = \frac{1}{16}\sum_{q=1}^{16}\min_{p\in H\,\mathrm{valido}(q)} R_{q,p}.
\]

Definimos:

\[
S_H=\frac{\sum_{c\in H}\mathrm{StorageProxy}(c)}{\mathrm{StorageProxy}(TS0)},\quad
W_H=\frac{\sum_{c\in H}\mathrm{KeysTouchedProxy}(c)}{\mathrm{KeysTouchedProxy}(TS0)}.
\]

O *score auxiliar* usado após filtrar a frente Pareto é:

\[
Score(H)=0.8R_H+0.1S_H+0.1W_H.
\]

Os valores 0,2/0,8 e 0,8/0,1/0,1 são **escolhas de política exemplificativas**, não calibração empírica. É fundamental congelar e divulgar essa política **antes** do benchmark.

O custo de manutenção real de TS5/TS6, a memória, a rede, o volume de SSTables, os custos de partição, falhas e concorrência **não** estão integralmente representados por esses números. A frente Pareto é apenas relativa aos proxies selecionados, não uma prova de dominância de desempenho real.

## 5. Dois perfis, duas conclusões diferentes

| Perfil | Interpretação | Como tratar TS5/TS6 |
|---|---|---|
| `conservative` | Não pressupõe o alinhamento de janelas aleatórias do TSBS | Não seleciona planos que exigem `alignment_guard`; permanecem candidatos brutos TS0/TS1/TS2 |
| `conditional_aligned` | Hipótese experimental a verificar *antes* de medir | Pode escolher rollups por minuto/hora; guarda as consultas que precisam de alinhamento, com fallback TS0 se a hipótese falhar |

**Nenhum perfil constitui “cobertura Cassandra real verificada”.** Para isso, ainda são necessários uma fonte de lista completa dos hosts, parâmetros concretos da consulta, dados materializados, manutenção durável, teste de CQL e equivalência das respostas.

## 6. Como entender as saídas

- `28_hbench_selected_deployments.json`: deployment completo, IDs das tabelas, métricas de *proxy*, fonte canônica, avisos, mapeamento de cada uma das 16 consultas, condição de alinhamento, dependência de WritePlan e identificador determinístico `H_...`.
- `29_hbench_selected_query_routes.json`: relações planificadas **deployment → consulta → QueryPlan**. Útil para gerar o executor Cassandra na Etapa 9.
- `30_hbench_summary.csv`: tabela resumida ordenada do HBench pré-benchmark, pronta para planilhas/artigo (rotular corretamente `analytical proxy`).
- `31_stage8_manifest.json`: contagens de enumeração, escopo, checksums de arquivos de entrada e advertências de correção.
- `32_stage9_execution_plan.csv`: localiza os arquivos CQL existentes da Etapa 4 e seus WritePlans para cada deployment. **Não é um script de implantação nem um plano executável de benchmark ainda.**

## 7. O que não foi implementado e o que deve vir depois

1. A métrica de 5% *near-best* e o *regret* dependem de latências medidas num espaço de referência comparável. Não são calculados nesta etapa.
2. A avaliação de dominância usa somente proxies e não deve eliminar alternativas da **avaliação-oráculo retrospectiva** quando formos estudar *design-space reduction* do SchemaLens.
3. Precisaremos executar cada implantação inteira: criar tabelas, povoar a MESMA workload TSBS, carregar também agregados, validar resultados e medir p50/p95/p99, RPS, ingestão, volume em disco, manutenção e escrita.
4. O esquema Cassandra original do TSBS deve ser executado **separadamente no mesmo ambiente**, com hardware, escala, versões, concorrência e conjunto lógico de consultas equivalentes.
5. Uma limitação metodológica atual é que o universo de nove candidatos físicos **não** representa todos os projetos Cassandra possíveis; ele é somente o catálogo produzido nas Etapas 3–4. Não chamar a enumeração desses nove de “ótimo global de Cassandra”.
6. Outras decisões ainda pendentes: verificações exatas de `lastpoint` multi-métrica, correspondência de timestamps, fontes canônicas duráveis, consistência de atualizações de agregado, catálogo de hosts, backfill e particionamento em cenários muito grandes.

O próximo trabalho operacional (Etapa 9) não deve usar o resultado do benchmark antigo para alterar regras TSDB, candidatos, pesos ou a lista Top-k **após** observar seus tempos. Para avaliação de regret, um oráculo retrospectivo independente pode medir alternativas de referência, mas esses números não devem retroalimentar a seleção em avaliação.
