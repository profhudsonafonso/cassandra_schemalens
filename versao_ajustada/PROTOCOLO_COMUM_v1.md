# Protocolo comum de execução v1 — SchemaLens/Cassandra (TSDB)

Congelado em 17/11/2026 (`common/protocol_v1.json`). Responde às divergências críticas e importantes da
análise `docs/Analise_Consistencia_Metodologica_Benchmarks.pdf` (Seções 6 e 7). A **fase de projeto** de cada
benchmark (ativação → candidatos → rotas → custo → HBench) já era consistente e não é refeita. Este protocolo
padroniza a **fase de medição** dos três TSDB (IoT-Benchmark, TPCx-IoT, TSBS). O RUBiS fica fora desta rodada.

| # | Item | Regra v1 | Divergência que corrige |
|---|---|---|---|
| 1 | Ambiente | uma imagem fixada por digest (`cassandra@sha256:25f6a4d5…`, release 4.1.12), container **dedicado e novo** por benchmark, nó único, RF = 1, CPUs 0–3, 6 GB, heap 2 GB; cliente Python nas CPUs 8–11; preflight grava versão, digest, driver, CPU/RAM, outros containers e loadavg | crítico 2 (versões diferentes, tag flutuante, compose × execução) |
| 2 | Deployments | todo o HBench + baseline(s) do benchmark; se o espaço admissível tiver ≤ 12 deployments, mede-se o espaço inteiro (modo verification); um keyspace por deployment; só `CREATE` | crítico 1 (TSBS 1/7) e crítico 4 |
| 3 | Repetições e ordem | 5 repetições; **reinício a frio** do container antes de cada uma; 10 aquecimentos + 60 rodadas cronometradas por (consulta, variante) e repetição; janela contínua; em cada rodada, ordem das consultas sorteada e, em cada consulta, permutação sorteada das variantes, invertida nas rodadas pares; janela interrompida ou que viola a guarda é descartada inteira e refeita | crítico 3 |
| 4 | Métricas | sempre média, p50, p95 e p99 (nearest rank); **métrica de decisão definida antes: média do mix uniforme** (média não ponderada das médias por classe); secundária: média dos p95 por classe | crítico 3 |
| 5 | Estatística | IC 95% por bootstrap percentil (2.000 reamostras, estratificado por consulta × repetição); IC t sobre as 5 médias por repetição; contrastes pareados (mesmos índices de rodada); **piso de ruído** = maior quantil 95% de \|ln razão\| entre pares A/A (deployments que executam trabalho idêntico); empate se o IC pareado contém 1 ou se \|ln razão\| < piso | crítico 3 |
| 6 | Corretude | oráculo lógico independente do layout, calculado das linhas-fonte em memória; checagem prévia de todas as rotas antes de cada repetição; **toda resposta cronometrada** comparada fora do cronômetro | importante 7 (TSBS não independente) |
| 7 | Estado físico | `nodetool flush` após a carga; **gate** antes de cada repetição (SSTable ≥ 1 e memtable vazia em todas as tabelas); **guarda de isolamento** (amostra `system_views.sstable_tasks` a cada 15 s + `system.compaction_history` antes/depois, exigindo 0); armazenamento por `nodetool tablestats` | importante 11 |
| 8 | Escrita | tempo de carga sequencial e número de escritas físicas por deployment (mesmo carregador para todas as variantes) | importante 8 |
| 9 | Avaliação do método | no espaço de referência medido: Top-1 (estrito e dentro do ruído), Top-3, near-best 5%, regret, retenção da fronteira de Pareto medida (leitura, bytes, escrita), τ de Kendall escore × medido, comparação com escolha aleatória de k | crítico 4 |
| 10 | Ablações | neutralizar cada regra TS0–TS8 e M1–M5 no motor de ativação (sem alterá-lo), propagar e rodar de novo o Stage 8 congelado; ablações de critério (sem diversidade, sem Pareto, k = 1, regra antiga, aleatório); avaliar com as medições do modo verification | importante 10 |
| 11 | Aceitação | gates recalculados dos dados brutos (completude, oráculo, isolamento, flush, ambiente, congelamento, Stage 7 × 9, piso de ruído, firewall, contrabalanço) + testes negativos | — |
| 12 | Firewall | nenhuma latência entra na seleção; pré-registro por SHA-256 (código de medição + fase de projeto) gravado em modo exclusivo antes de medir; a medição recusa rodar se algo mudou | — |

## O que este protocolo não harmoniza (deve ser declarado)

* **Modelo de custo e algoritmo de seleção** (importantes 5 e 6): continuam próprios de cada benchmark, porque fazem parte da fase de projeto congelada. O protocolo mede o efeito deles (Top-k, regret, ablações), mas não os unifica.
* **Natureza do baseline** (importante 9): cada TSDB compara com o esquema de referência do próprio benchmark, não com métodos da literatura.
* **Origem e escala dos dados** (menores 12 e 14): continuam as de cada benchmark e ficam declaradas nos relatórios.
* **Latência**: ponta a ponta no cliente Python, que inclui driver e pós-processamento; não é tempo de servidor.
