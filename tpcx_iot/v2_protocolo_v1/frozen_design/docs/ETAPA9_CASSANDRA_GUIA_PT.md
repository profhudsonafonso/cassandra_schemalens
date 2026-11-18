# Etapa 9 — Apache Cassandra: validação real e benchmark piloto do SchemaLens

**Status deste pacote:** código + `dry-run` + 122 testes offline aprovados. **SEM medições Cassandra neste ambiente de construção.** Para produzir dados reais, o usuário precisa executar `smoke` e depois `benchmark` num Cassandra em funcionamento.

## 1. Continuidade com o artigo original (DKE)

O Stage9 **não reativa templates** e **não modifica HBench**. Os artefatos das etapas anteriores são entradas imutáveis: matriz analítica, candidatos físicos, QueryPlans, WritePlans, proxies de custo e a seleção de cinco deployments completos (`54_hbench_selected.json`). O artigo DKE distingue a estimativa pré-benchmark do desempenho medido após materializar uma configuração completa; este piloto implementa esta separação. Os resultados Stage9 são exclusivamente **posteriores à seleção** e NÃO entram nas regras `TS0–TS8`, `M1–M5` ou na ordenação de `HBench`.

### HBench congelado

| Nome curto | Deployment ID | Materialização | Leitura principal |
|---|---|---|---|
| H1 | `h_119222012d4e` | TS0 | TS0 |
| H2 | `h_6745757bf865` | TS1-3600s + TS3-3600s | TS3-3600s |
| H3 | `h_a9983e2318a9` | TS1-60s + TS3-60s | TS3-60s |
| H4 | `h_3f1449e05468` | TS0 + TS3-3600s | TS3-3600s |
| H5 | `h_aba788b8e7b7` | TS1-3600s | TS1-3600s |

Os identificadores são herdados da etapa 8, não gerados no Stage9.

## 2. Limite importantíssimo: adaptação ≠ benchmark TPCx-IoT oficial

Este é um **microbenchmark acadêmico reprodutível inspirado no TPCx-IoT 2.1.2**; NÃO é uma execução certificada do TPC.

A especificação oficial, cláusula 2.2, descreve um sensor selecionado e duas janelas de 5 segundos, mas a forma exata de filtro (nome do sensor apenas versus **subestação + sensor**) e a seleção de agregações ainda precisam ser comprovadas no código oficial licenciado do kit. Uma descrição acadêmica original relata quatro agregações (`MAX`, `MIN`, `AVG`, `COUNT`) por subestação/sensor; a implementação congelada da Stage3 utiliza **leitura bruta por nome do sensor**, definida no SQL `inputs/workload/sql/Q_READ_2_WINDOWS.sql`. Não alteramos essa hipótese retrospectivamente.

**O que será validado aqui:** a equivalência do resultado exato da consulta congelada sobre as cinco materializações, sob entradas sintéticas controladas. **O que NÃO será demonstrado:** conformidade com TPC, taxa oficial `IoTps`, comportamento estatístico do driver TPC, tolerância a perda do host da fila SQLite, consistência distribuída, duas réplicas em um cluster local, custo real de migração, extrapolação para todas as 200 classes de sensor no piloto padrão.

Fontes: [Especificação oficial TPCx-IoT v2.1.2](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPCX-IOT_v2.1.2.pdf) e [página oficial do benchmark](https://www.tpc.org/tpcx-iot/default5.asp).

## 3. Preparar o Ubuntu e o VS Code

Requisitos: Docker Engine + plugin Docker Compose, Python 3.10–3.12 recomendado, venv, porta local 9042 livre, aproximadamente 2 GiB livres para o piloto, salvo mais recursos para escalas maiores.

```bash
cd SchemaLens_TPCx_IoT_Cassandra_Stage9
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-stage9.txt

# PASSO 1: só inspeção; não usa Cassandra
python -m methodology.tpcx_iot.stage9.cli dry-run

# PASSO 2: deve apresentar 122 testes aprovados
python -m unittest discover -s tests -v

# PASSO 3: subir Cassandra local de um nó (RF=1, APENAS PESQUISA)
docker compose -f docker-compose.stage9.yml up -d
# Aguarde health check atingir healthy, pode levar alguns minutos
docker ps
# Se necessário: docker logs schemalens_tpcx_cassandra
```

O `docker-compose.stage9.yml` usa Cassandra 4.1.8, uma instância, keyspaces de pesquisa com `SimpleStrategy`, RF=1. **O TPCx-IoT oficial requer pelo menos duas réplicas; este ambiente não atende à exigência.**

## 4. Executar testes reais de equivalência — smoke

**Aviso destrutivo:** `--reset` remove SOMENTE os keyspaces do experimento com prefixo `sl_tpcx_*` e reinicia os arquivos locais de replay correspondentes. Use uma instância descartável, não um Cassandra de produção.

```bash
python -m methodology.tpcx_iot.stage9.cli smoke --reset
```

Para cada H1–H5, o script:

1. Verifica os hashes/IDs de HBench e associa os WritePlans corretos.
2. Cria um keyspace isolado (`sl_tpcx_h_<id>`) com a DDL oficial congelada do Stage4.
3. Gera 780 eventos sintéticos determinísticos (2 subestações × 3 sensores × 130 instantes) com timestamps UTC e `event_id` UUID estável.
4. Persiste cada evento no outbox SQLite local antes das operações Cassandra; executa toda escrita canônica/projeção requerida e confirma por tabela.
5. Executa as duas janelas fechadas [11s,16s] e [58s,63s] para cada sensor; a janela recente cruza o limite de buckets de 60 segundos.
6. Recupera todas as páginas CQL, funde os resultados usando a identidade completa dos eventos e compara o resultado (com multiplicidade) ao oráculo independente.
7. Em **smoke pequeno**, verifica também `SELECT COUNT(*)` de cada tabela física. Isso não é recomendado para produção em grande escala.
8. Só marca `PASS_LIVE_SEE_LIMITATIONS` se os resultados forem exatamente equivalentes.

É possível variar cardinalidade e tamanho da amostra: `--stations 5 --sensors 4 --seed-steps 200`. Estes parâmetros NÃO são o scale factor oficial do TPCx-IoT.

Se ocorrer uma falha parcial que já tenha persistido eventos no outbox, é possível tentar a recuperação **sem limpar os dados**:

```bash
python -m methodology.tpcx_iot.stage9.cli repair
```

O modo de recuperação requer que os arquivos SQLite sobrevivam; ele refaz as mutações ainda não confirmadas com a mesma identidade. Não prova atomicidade entre partições, sobrevivência à perda física do host nem consistência sob múltiplos produtores concorrentes. A contagem `COUNT(*)` é desabilitada no modo de reparo porque uma carga de benchmark parcialmente concluída pode ter medições adicionais fora do `seed`.

## 5. Medir a carga mista real — benchmark de pesquisa

Apenas após os cinco testes `smoke` passarem:

```bash
python -m methodology.tpcx_iot.stage9.cli benchmark --reset --bench-steps 500 --read-threads 2
```

O benchmark prepara os mesmos 780 eventos, valida os dados e então inicia dois workers de leitura enquanto grava **3.000 novos eventos lógicos** (500 instantes × 2 subestações × 3 sensores), incluindo TODAS as tabelas de cada deployment. As leituras são feitas em janelas fixas dos dados já verificados. Assim, os workers executam simultaneamente com a ingestão e a correção das respostas pode ser comprovada sem depender da ordem incerta de chegada das novas observações.

A métrica `research_ingest_rows_per_second` é eventos **lógicos** por segundo (incluindo a sobrecarga do SQLite outbox), **não** a métrica oficial `IoTps` do TPC. Um evento inserido em TS0+TS3 produz duas mutações físicas e conta como **um** evento lógico.

O benchmark registra p50/p95/p99 da **consulta lógica completa** (duas janelas, todas as consultas CQL, todas as páginas e merge dos resultados), **excluindo o custo de verificação contra o oráculo no temporizador**. As leituras ficam ancoradas nas janelas do seed durante a ingestão. Isso não substitui o comportamento temporal dinâmico do driver oficial.

Opcionalmente, coletar saída bruta `nodetool tablestats` de cada keyspace:

```bash
python -m methodology.tpcx_iot.stage9.cli benchmark --reset --bench-steps 500 --read-threads 2 \
  --nodetool-container schemalens_tpcx_cassandra
```

A saída bruta é salva como `64_nodetool_<deployment_id>.txt`. Não usamos estimativas de armazenamento do Stage7 como armazenamento realmente medido.

**Cuidado estatístico:** poucos eventos ou pouquíssimas consultas não permitem afirmar estabilidade de p95/p99. É necessário repetir medições controladas, registrar versões e recursos, testar ordenações diferentes dos deployments, aquecimento, múltiplas escalas, política de cache, replicação e consistência.

## 6. Saídas

Dentro de `outputs/tpcx_iot_stage9/`:

- `60_stage9_execution_manifest.json` — seleção imutável, hashes de origem, plano dos cinco deployments, estado dry-run.
- `61_stage9_live_results.json` — **só aparece após tentativa live**; inclui prova de equivalência, detalhes da medição ou erro explícito.
- `62_stage9_repair_results.json` — resultado operacional do replay, quando executado.
- `63_stage9_summary.csv` — resultados de smoke/benchmark por deployment (somente após execução live completa).
- `64_nodetool_<id>.txt` — armazenamento real em relatórios brutos (opcional).
- `local_outbox/<keyspace>.sqlite` — estado de recuperação do piloto; nunca subir para GitHub por padrão.

Se o arquivo `61_stage9_live_results.json` não existir ou tiver `status=FAILED_NOT_REPORTABLE`, NÃO há resultado válido de desempenho para publicar.

## 7. Limites e próximos experimentos necessários para artigo

- **Gatilho 1 (kit):** conferir o driver oficial do TPCx-IoT. Se usar `driver_system_id + sensor_name` ou agregações, documentar versão revisada de EER/workload/regras e executar uma nova pré-seleção, sem usar os resultados atuais como treinamento/otimização.
- **Gatilho 2 (replicação):** usar no mínimo dois nós de Cassandra para experimentos que aleguem replicação, fixando `replication_factor`, consistência e concorrência para todas as configurações.
- **Gatilho 3 (robustez):** medir falhas parciais e replay com reinício real; não afirmar atomicidade distribuída com SQLite local.
- **Gatilho 4 (preservação):** medir os outros quatro deployments condicionalmente admissíveis de Stage8 ou outra referência pré-declarada antes de calcular Top-k/near-best/regret. Medir apenas HBench permite identificar o melhor **entre os cinco**, mas não provar preservação relativamente aos nove.
- **Gatilho 5 (repetições):** definir um protocolo formal com replicação de runs, scales comparáveis, randomização controlada da ordem, warmup, métricas de storage e tratamento de falhas.

A pergunta científica permanece: o SchemaLens pode reduzir o espaço de design temporal sem eliminar boas configurações? Não inferir essa resposta de diferenças de contagem de CQL ou do ganho de uma única execução.
