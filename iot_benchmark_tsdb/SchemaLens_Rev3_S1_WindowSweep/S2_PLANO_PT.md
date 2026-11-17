# S2 — protocolo proposto para crescimento real do histórico (após S1)

## Pergunta de pesquisa

Quando o crescimento do **número de observações por dispositivo**, mantidos fixos os demais fatores, torna a TS2-D competitiva ou preferível à TS2-U em latência, particionamento, memória e armazenamento?

Este arquivo é um **protocolo prospectivo**, não um resultado nem uma implementação já executada. Nenhum novo dataset Cassandra é gerado pelo pacote S1.

## Desenho por etapas

**Efeito 1 — crescimento do histórico, sem variar o número de dispositivos.** Manter nove dispositivos, dois sensores DOUBLE, lotes de cinco, `POINT_STEP=600000 ms` (10 minutos por timestamp), a mesma política de compressão, driver, paginação, hardware e consultas temporais de 1, 7 e 14 dias. As escalas propostas para futuras gerações Java são:

| Escala | Loops por dispositivo | Timestamp por dispositivo (loops × 5) | Células métricas (9 dispositivos × 2 sensores) | Duração aproximada |
|---|---:|---:|---:|---:|
| S2-H1 | 600 | 3.000 | 54.000 | 20,8 dias |
| S2-H2 | 2.400 | 12.000 | 216.000 | 83,3 dias |
| S2-H3 | 9.600 | 48.000 | 864.000 | 333 dias |

As escalas H2/H3 exigem aumentar o `max_cells` do gerador em **um novo protocolo**, antes da geração; não devem reutilizar nem modificar A/B. Usar novas seeds determinísticas distintas por escala e entre `DATA_SEED`/`QUERY_SEED`, registradas antes de medir. Não interpretar valores desta tabela como quantidade efetivamente gerada até auditar os CSVs Java.

**Efeito 2 — comprimento da janela.** Em cada escala, variar separadamente a janela 1/7/14 dias usando os **mesmos dados físicos** de cada escala; caso necessário, um controle de histórico integral. Evitar misturar mudanças no histórico com mudanças na janela ao comparar arquiteturas.

**Efeito 3 — número de dispositivos.** Só depois, com o mesmo histórico por dispositivo, comparar 3/9/18 dispositivos. Esse fator aumenta o volume total, mas não deve alterar o tamanho individual das partições para determinada arquitetura.

**Efeito 4 — densidade temporal.** Se o objetivo for realmente testar limites próximos de 32 MiB por partição, variar `POINT_STEP` de forma separada, com mais timestamps por partição e várias escalas de volume. Na campanha B, a maior partição TS2-U era ~105.778 bytes, aproximadamente 0,3% do limite suave de 32 MiB. Seriam necessários centenas de vezes mais bytes por partição para testar esse limite; não concluir um limiar só com H1–H3. A compressão não necessariamente escala linearmente com o número de linhas.

## Procedimentos obrigatórios

- Gerar cada dataset no Java IoT Benchmark fixado em `62e0dc100e56443978c8b4c66472b1d8a7ef0238` (ou registrar formalmente mudança de versão); capturar múltiplos objetos por classe numa futura evolução, sem declarar replay oficial de SQL.
- Criar um plano e uma seleção por escala **antes** da latência. Caso o experimento seja de sensibilidade de planos fixos, preservar explicitamente a seleção original e não interpretá-la como escolha nova.
- Validar todos os resultados contra o oráculo; conferir `plan_id`, requisições/linhas iteradas, cardinalidade, correção de intervalos e chaves de particionamento.
- Criar keyspaces novos e não sobrescrever os das campanhas Rev3 A/B; medir p95 por Q, média/mediana, p99, latência e custo de escritas, com alternância balanceada de execução.
- Fazer *flush* **somente depois de concluir latências**; coletar SSTables, memtables e máximo/média de tamanho das partições. Separar métricas antes e depois de flush e documentar compaction/compressão/GC/cache.
- Calcular diferenças entre TS2-U e TS2-D por consulta e janela, não apenas o agregado. Reportar contagem de operações e número de partições; não usar contadores Python como substituto de server-side reads.
- Repetir em pelo menos duas seeds por escala quando o orçamento computacional permitir, distinguindo réplicas da fonte, repetições de query e replicação de execução.

## Critério de interpretação

TS2-D só poderá ser considerada preferível sob condições observadas se sua vantagem (desempenho e/ou limite de tamanho de partição) compensar as chamadas adicionais, armazenamentos e custos de manutenção para o workload explicitamente definido. Não adaptar pesos/proxies de Stage 7 com dados usados na mesma validação; uma Rev4 calibrada deve ser testada num holdout separado.
