# Etapa 7 — SchemaLens + TSBS: custos analíticos e restrições (TSDB-only)

## 1. Por que esta etapa existe?

As Etapas 3–6 transformaram evidência da workload TSBS `cpu-only` em famílias temporais ativadas, nove `CassandraCandidateSpec`, 37 planos de leitura e nove WritePlans. Agora extraímos **evidências quantitativas aproximadas** e advertências de viabilidade para decidir quais análises devem entrar no estágio de consolidação. As estimativas não demonstram que um candidato será rápido, não são latências e não substituem benchmarks.

Fonte do cenário: `inputs/schema/tsbs_cpu_only_schema.json` (Etapa 3): 100 hosts, dez métricas densas, coleta a cada 60 segundos e duração de três dias; cenário proposto para o experimento, não parâmetro nativo fixo do TSBS. Um início ilustrativo `2016-01-01T00:00:00Z` torna reproduzível a distribuição de partições de 7 dias alinhadas à época Unix. Esses dados são explícitos e auditáveis.

## 2. Arquitetura incremental

```
Etapa 3: regras TS0–TS8 e M1–M5 (IMUTÁVEIS)
       ↓
Etapa 4: CassandraCandidateSpec [9 candidatos]
       ↓
Etapa 5: QueryPlan [37 vínculos]
       ↓
Etapa 6: WritePlan [9 candidatos]
       ↓
Etapa 7: Estimativa de footprint/read fanout/write touched keys + guards
       ↓
Etapa 8 (futura): consolidação e HBench (após definir política de custos)
       ↓
Etapa 9 (futura): Cassandra/TSBS medido em hardware controlado
```

O algoritmo nesta etapa NÃO ativa famílias, NÃO materializa configurações compostas, NÃO executa operações do Cassandra e NÃO ranqueia candidatos. Não há arquivo substituído.

## 3. Arquivos acrescentados e como ler o código

- `config/tsbs_stage7_cost_policy.json`: pressupostos de tamanho lógico de tipos CQL e limiares que funcionam **apenas como alertas**. Não alteram TS0–TS8.
- `inputs/tsbs_stage7_mozaffari_evidence.json`: recipiente para FR/FU/RC/TI direcional. Os campos continuam desconhecidos (`null`) e são reportados sem inferência indevida.
- `methodology/tsbs/stage7/estimator.py`: valida identidade, hash e consistência dos insumos, constrói projeções analíticas de partições, linhas e operações e grava saídas.
- `methodology/tsbs/stage7/operators.py`: descreve transformações físicas com rótulos explicativos. Não implementa de maneira fiel os operadores e a função objetivo de NoSE/Chen.
- `methodology/tsbs/stage7/cli.py` e `run_tsbs_stage7.sh`: entradas de execução.
- `examples/show_stage7_costs.py`: demonstra exemplos pequenos de leitura dos resultados; não gera benchmark.
- `tests/test_tsbs_stage7.py`: regressão, verificação de números hipotéticos, auditoria de sinais, proteção contra ranking e mudanças upstream.

## 4. As projeções de armazenamento

Modelos analíticos de exemplo (antes de byte proxy):

- TS0, TS1: `hosts × metrics × pontos_por_host` linhas. TS1 divide por partições temporais.
- TS2: `hosts × pontos_por_host` linhas contendo dez métricas na mesma linha.
- TS5 MAX/minuto: uma linha por host × métrica × minuto com dados; MAX/hora e SUM/COUNT usam hora.
- TS5 global MAX/minuto: uma linha por minuto/medição `usage_user` no cenário estudado.
- TS6: uma linha por (dia, hora, host), mas cada partição física corresponde a uma (hora, dia) e acumula hosts.

**Exemplos**: para 100 hosts, 10 métricas, 3 dias e 1 leitura/minuto/host, TS0 tem 4.320.000 linhas físicas e TS2 tem 432.000 linhas mais largas. Essas quantidades não implicam automaticamente menos bytes ou menor p95 no TS2. O bucket de TS1 de um dia fica com cerca de 1.440 linhas por host/métrica/dia. Em TS0 cada partição tem 4.320 linhas nesse horizonte **e cresce com a retenção**.

O proxy de tamanho de linha usa soma de larguras lógicas por coluna com um overhead suposto, declarado em JSON. Ignora metadados internos Cassandra, SSTables, compressão, replicas, TTL, tombstones e índices. Não deve ser apresentado como espaço real em disco.

## 5. Projeções de leitura e custo

Para Q13 (`high-cpu-1`, dez métricas, um host, 12 horas), o cenário de um ponto/minuto gera aproximadamente:

| Candidato | CQL requests | Linhas físicas consumidas, proxy |
|---|---:|---:|
| TS0 | 10 | 7.200 |
| TS1 (1 dia) | <= 20 (conservador se cruzar dia) | 7.200 |
| TS2 (1 dia) | <= 2 (conservador se cruzar dia) | 720 |

**Importante:** há perda proposital de precisão em favor de um teto de fan-out quando o TSBS sorteia janelas não fixadas. Isso não fornece p95, número real de bytes lidos nem garantia de vantagem do TS2.

Para Q10 (`double-groupby-1`, todas as 100 hosts, 12 horas), TS6 tem até 12 requisições por hora, com 100 linhas por hora. Porém só serve diretamente intervalos alinhados à hora; a Etapa 5 marca esse guarda. Se o TSBS gerar intervalo não alinhado, o plano de rollup sozinho não é semanticamente suficiente. A Etapa 8 não pode escolher tal plano como cobertura incondicional.

Para Q16, o TS5 global lê até cinco registros em cada partição diária visitada para recuperar os cinco últimos minutos válidos. A versão bruta TS0 pode precisar recuperar dados históricos de todos os hosts. As estimativas usam o horizonte completo ilustrativo porque o instante de consulta aleatório real não está disponível.

## 6. Projeções de escrita e manutenção

A métrica `distinct_physical_group_rows_touched_by_new_observation_proxy` é contagem de **chaves potencialmente afetadas** por uma nova observação completa de dez métricas; não o número de CQL emitidos. TS0/TS1 tocam dez linhas, TS2 uma, TS5 por host/métrica dez, TS5 global uma, TS6 uma. O código da Etapa 6 pode omitir escrita se o valor agregado não mudou, mas pode emitir DELETE em eventos removidos. Logo, **write amplification real ainda é desconhecida**.

Os agregados TS5/TS6 dependem de histórico canônico e recomputação de grupos afetados; tamanho/custo dessa fonte é omitido dos proxies de espaço. Em Cassandra real são necessárias durabilidade, consistência, gerenciamento de concorrência e recuperação após falhas.

## 7. Mozaffari, NoSE e Chen/QPG: fronteiras conceituais

O documento teórico SmartRetail V2 afirma que a metodologia atual não conhece os sinais de Mozaffari:

- **FR**: frequência direcional de leituras;
- **FU**: frequência de update/delete da classe;
- **RC**: cardinalidade média direcional da relação (NÃO `schema_rc`);
- **TI**: multiplicidade temporal da classe/relação (NÃO frequência de amostragem).

O add-on reporta presença/ausência desses quatro sinais por direção, mas não calcula `Cost_R` e `Cost_D`. Mesmo com todos os sinais preenchidos, a fórmula publicada ainda não foi implementada — isso é declarado no relatório, não simulado.

Os rótulos de `operators.py` indicam preservação de séries brutas, bucket temporal, co-localização de métricas, materialização de rollup e de dimensão. NoSE oferece uma linha de inspiração para operadores de projeto; **os algoritmos Materialize/Decompose/Relax, seu modelo de custo e a otimização de Chen/QPG não estão sendo executados**.

## 8. Cuidados metodológicos para reunião com supervisores

1. Não tratar estes nove candidatos como configurações HBench ou implantações completas; cada estrutura derivada precisa de fonte canônica.
2. Não usar os custos estimados como p95 ou custo em moeda.
3. Não assumir que uma mesma resolução de rollup responde toda janela do TSBS. Guardas de alinhamento permanecem.
4. Não supor que o dataset de três dias e 100 hosts foi carregado no Cassandra.
5. Não substituir valores ausentes de FR/FU/RC/TI com estimativas de frequência de amostragem.
6. Não usar benchmarks externos/anteriores para escolher limiares de ativação ou ranquear candidatos.

## 9. Como executar e reproduzir

```bash
python3 -m methodology.tsbs.stage7.cli
python3 examples/show_stage7_costs.py
python3 -m unittest discover -s tests -v
```

Consulte `outputs/tsbs_stage7/27_stage7_manifest.json` para os hashes SHA256 de todos os insumos e a lista explícita de limitações. A **próxima Etapa 8** deverá validar consolidação de múltiplas consultas, conflitos, inclusão de dependências de escrita, QueryPlans condicionais e cobertura completa; HBench só fará sentido com uma política de custo previamente fixada e custos mensuráveis, não com suposições disfarçadas de medição.
