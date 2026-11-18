# Adaptação SchemaLens para IoT Benchmark / Cassandra — Guia técnico

## Princípio

O objetivo é preservar a separação formal SchemaLens `A(q) -> P -> C -> H -> Hbench -> medições`. O benchmark não escolhe famílias. Um resultado experimental do Stage 9 não pode reordenar nem redefinir o catálogo ou o HBench pré-selecionado. A escolha do banco continua sendo Cassandra (opção A).

## Evidências empregadas

- Regras de ativação TS0–TS8 e modificadores M1–M5: **código original A1** (vendorizado).
- IoT Benchmark: arquivos `schema.txt`, `info.txt`, `BigBatch_*.csv` e classes de operações Q1–Q12. Fonte pública [IoT Benchmark](https://github.com/thulab/iot-benchmark); funções Java concretas `CSVDataWriter`, `CSVSchemaWriter`, `GenerateQueryWorkLoad`.
- Adaptadores dos Stages 4–9: arquivos `iot_pipeline/`. O código TSBS original está arquivado sem alteração. Reutilizamos contratos e decisões metodológicas, não fingimos que seu leitor CPU-only suporta IoT arbitrário.

## Etapa 4 — instanciação

A relação ativação-família gera uma especificação Cassandra somente se há instanciador físico implementado. Para a entrada ilustrativa: `TS0`, `TS1`, `TS2`, `TS4`, `TS5`. Ao contrário de ativação, um CQL DDL sozinho não prova que uma consulta é executável. Não usar `C0–C9`.

## Etapa 5 — leitura e cobertura

Q1–Q12 são vinculadas a opções fisicamente materializadas. Q4–Q6 são atendidas pela TS0 (com agregação no cliente). Q12 usa operações de conjunto após recuperar as linhas. `TS4` precisa TS0 como fallback quando o snapshot global é posterior ao fim de uma janela histórica. `TS5` exige janela completa/alinhada e resolução consistente; o exemplo A1 tem `end = último_timestamp+1`, então seus grupos são parciais. O código rejeita usar TS5 como caminho direto nesses casos.

O Stage5 verifica elegibilidade estrutural; `iot_pipeline/offline.py` valida resultados em memória contra o oráculo A1; o Stage9 verifica a equivalência após executar CQL verdadeiro.

## Etapa 6 — escrita e manutenção

`TS0` é a fonte canônica. `TS1` cria células por dispositivo/sensor/dia. `TS2` materializa observações por dispositivo/dia/timestamp com valores de diferentes sensores em colunas. `TS4` armazena o último ponto; `TS5` mantém sum/count/min/max por janela. Para correções/backfill, o protótipo materializa novamente a fonte canônica, não aplica deltas incorretos. Não há protocolo distribuído de atualização concorrente/compensação em falha: não apresentar como produção.

## Etapa 7 — estimação antes de medir

Três dimensões, todas estimadas: leitura (número de requisições e linhas lógicas), armazenamento (contagem de linhas × bytes hipotéticos por família), escrita (chaves afetadas). Pesos fixos e fatores de famílias ficam em `config/pipeline_policy.json`. Eles não são latências preditas em ms e não devem ser calibrados com resultados do Stage9. Fatores e volumes do fixture não garantem boa generalização.

## Etapa 8 — cobertura global e Pareto

Enumeramos as combinações viáveis das famílias concretas, exigindo `TS0` por necessidade de fonte canônica e fallback; toda consulta deve possuir uma rota válida. Cada deployment é identificável por hash dos candidatos físicos e recebe a escolha de QueryPlan por consulta. A seleção Pareto usa três objetivos e `top_k=5` como limite, não exige fabricar cinco alternativas. Na execução fornecida: 4 HBench de 16 composições possíveis. `Hbench` ainda **não é um ranking empírico**.

## Etapa 9 — somente no computador do usuário

O driver Python é acionado explicitamente com `--live`, exige confirmação exata antes de apagar **somente keyspaces de experimento** `sl_iot_h_*`, cria tabelas, importa o dataset normalizado, aplica a rota congelada, compara toda resposta com o oráculo e, se todas passarem, mede amostras temporizadas sequenciais. Os arquivos de latências brutas e p95 têm proveniência Stage8 via hashes. Nenhuma saída de medição escreve de volta nos Stages 4–8.

## Limitações científicas a superar antes de publicar

1. Reproduzir exatamente `GenerateQueryWorkLoad` e as consultas SQL geradas pela versão fixada do IoT Benchmark, não apenas os rótulos Q1–Q12; Q8/Q12 precisam de semântica e projeções oficiais.
2. Aumentar o volume além do fixture de 35 medidas e introduzir cenários por escalas, frequência de escrita e leituras concorrentes.
3. Medir ingestão, armazenamento real, falhas, cold/hot runs, latência de manutenção TS4/TS5 e carga multi-cliente; comparar o mesmo contrato lógico e parâmetros.
4. Adicionar tipos de sensores heterogêneos (além de DOUBLE/ms) e consultas realistas sobre múltiplos dispositivos.
5. Só calcular Top-k-preservation, regret e near-best (≤5%) após executar referências comparáveis independentes; jamais usá-las na escolha HBench.

**Conclusão atual:** temos o caminho analítico até HBench e a estrutura do executor local, com verificação de contratos e oráculo de fixture. Ainda não temos medição Cassandra e não temos o benchmark oficial IoT executado no Cassandra.
