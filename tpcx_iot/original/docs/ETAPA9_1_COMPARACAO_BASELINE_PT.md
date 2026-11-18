# SchemaLens + TPCx-IoT: Etapa 9.1 — Cassandra, carga temporal dinâmica e baseline

## O que mudou da etapa 9

A primeira execução `benchmark --bench-steps 500 --read-threads 2` teve **cinco HBench realmente executados** em Cassandra 4.1.12. Os resultados desse piloto tinham **leituras fixas sobre o seed** e número desigual de consultas entre deployments; logo, não devem sustentar a conclusão de que um candidato domina o outro.

Esta etapa preserva INTACTOS os artefatos das etapas 1–8, especialmente `outputs/tpcx_iot_stage8/54_hbench_selected.json`, `58_stage8_manifest.json`, QueryPlans, WritePlans e CQL. Nenhuma latência passada influencia a ativação ou a pré-seleção.

1. **Cenário controlado:** a mesma sequência de eventos por subestação, sensor e passo de tempo, e o mesmo número `--read-operations` de consultas lógicas por deployment.
2. **Janelas temporais dinâmicas:** a janela recente termina no último timestamp de um passo inteiramente inserido; a histórica, de 5 segundos, é escolhida por hash determinístico dentro dos 1.800 segundos anteriores. Trata-se de *relógio lógico de evento*, não o instante da máquina definido pelo kit oficial.
3. **Concorrência real:** duas ou mais threads de consulta trabalham ao mesmo tempo que o writer insere novos eventos e todas as projeções. A leitura de um checkpoint não muda com inserts futuros porque o estudo permite apenas eventos append-only em ordem temporal.
4. **Repetições e ordem:** `--repetitions` repete a medição após reset; a ordem de deployments é rotacionada determinísticamente para reduzir vieses de ordem.
5. **Auditoria pós-ingestão:** contagem de todas as tabelas, comparação da leitura de janelas em cada checkpoint e, com conjunto controlado até o limite configurado, varredura de **cada registro físico e cada coluna** contra o oráculo independente.
6. **Armazenamento:** após `nodetool flush`, coleta `nodetool tablestats` separadamente por tabela e soma `Space used (live)` para cada deployment. Valor físico é diferente da estimativa lógica da etapa 7; não inclui todo o overhead do cluster.
7. **Baseline externo:** modelo lógico de registro do TPCx-IoT convertido em uma tabela Cassandra sem bucketing e sem duplicação, particionada pelo sensor. Não foi gerado pelo SchemaLens; entra somente na comparação final.

## Importante: o que significa "schema original do benchmark"?

A especificação TPCx-IoT v2.1.2 (Cláusula 2.2) exige os atributos lógicos:
`driver system id`, `sensor name`, `timestamp`, `sensor reading` e `padding` (~1 KB). A **especificação não prescreve um único esquema físico** — há bindings por banco (HBase, Couchbase, Machbase, Lindorm, TimechoDB). O TPC não lista Cassandra como banco diretamente suportado.

O arquivo `inputs/baselines/tpcx_logical_raw_cassandra.json` formaliza, portanto, um **baseline Cassandra derivado do esquema lógico** (não um esquema físico oficial TPCx-IoT):

```sql
CREATE TABLE IF NOT EXISTS b_tpcx_logical_raw (
    driver_system_id text,
    sensor_name text,
    event_ts bigint,
    event_id uuid,
    sensor_reading double,
    padding text,
    PRIMARY KEY ((sensor_name), event_ts, driver_system_id, event_id)
) WITH CLUSTERING ORDER BY (
    event_ts ASC, driver_system_id ASC, event_id ASC
);
```

`event_id` é uma chave suplementar da reprodução acadêmica para preservar a multiplicidade de leituras com o mesmo timestamp. O baseline preserva todos os cinco atributos definidos pelo TPC. A escolha de `sensor_name` na PK reproduz o **predicado do workload analítico congelado nas etapas 1–8**, cuja correspondência com o binding oficial ainda precisa de verificação.

**Não escreva no artigo "Cassandra original do TPCx-IoT"; escreva "TPCx-IoT logical-record baseline implemented in Cassandra (research adaptation)".** Esta distinção evita atribuir ao TPC uma decisão física que a especificação não toma.

Outras referências: [especificação](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPCX-IOT_v2.1.2.pdf), [página do TPC](https://www.tpc.org/tpcx-iot/default5.asp).

## 1. Ambiente no VS Code — Ubuntu

Abra esta pasta e utilize o mesmo virtualenv Cassandra da etapa 9:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-stage9.txt

docker compose -f docker-compose.stage9.yml up -d

docker ps
python -m unittest discover -s tests -v
```

O Docker Compose local oferece somente **um nó e RF=1**; isso **não satisfaz o requisito oficial de replicação mínima dupla**. Manter RF=1 para o experimento local é válido apenas como reprodução de pesquisa, não TPCx-IoT oficial.

## 2. Inspecionar primeiro: sem tocar no Cassandra

```bash
python -m methodology.tpcx_iot.stage91.cli dry-run \
  --output outputs/stage91_dry_run
```

O manifest deve listar **seis** designs: H1, H2, H3, H4, H5 e `b_tpcx_logical_raw`. Os cinco HBench possuem identificadores congelados, e o baseline está marcado como externo.

## 3. Rodada de integração curta com baseline (destrutiva!)

> `--reset` apaga apenas keyspaces com o prefixo configurado e os outboxes desta execução. Use **somente** um Cassandra de benchmark descartável.

```bash
python -m methodology.tpcx_iot.stage91.cli benchmark \
  --reset --stations 2 --sensors 3 \
  --seed-steps 130 --bench-steps 30 \
  --read-operations 12 --read-threads 2 --repetitions 1 \
  --nodetool-container schemalens_tpcx_cassandra \
  --require-storage --require-full-audit \
  --output outputs/stage91_pilot
```

Verifique seis logs `rep=1/1` e estes arquivos:

- `71_stage91_live_runs.json`: resultados crus por execução; exige status `COMPLETE_RESEARCH_ONLY`.
- `72_stage91_summary.csv`: estatísticas por design (número de repetições informado).
- `73_stage91_vs_tpc_logical_baseline.csv`: razões de ingestão, p95 e armazenamento em relação ao baseline externo.
- `74_stage91_analysis.json`: estrutura da comparação. Ainda **não calcula Top-k** contra nove alternativas.
- `storage/repeat0/nodetool_*.txt`: saídas literais do Cassandra antes de parsing.

## 4. Rodada principal: 3 repetições x 6 designs

```bash
python -m methodology.tpcx_iot.stage91.cli benchmark \
  --reset --stations 2 --sensors 3 \
  --seed-steps 130 --bench-steps 500 \
  --read-operations 120 --read-threads 2 --repetitions 3 \
  --nodetool-container schemalens_tpcx_cassandra \
  --require-storage --require-full-audit \
  --output outputs/stage91_main
```

Por configuração, os eventos de origem serão `(130 + 500) × 2 × 3 = 3780`, dos quais `3000` estão na fase medida. Cada execução completa terá `120` consultas lógicas de duas janelas (o número de requisições CQL físicas poderá variar; isso faz parte do design). Nenhum número é pré-calculado como se fosse medição real.

**Interpretação das razões**:

- `ingest_ratio_vs_baseline > 1`: SchemaLens HBench ingeriu mais eventos/segundo no benchmark experimental.
- `read_p95_ratio_vs_baseline < 1`: p95 menor que o baseline.
- `storage_ratio_vs_baseline > 1`: consumiu mais bytes live no Cassandra.

A rotina atual usa **mediana da métrica de cada repetição** e não fornece testes de significância ou intervalos de confiança. A etapa seguinte pode incluir mais repetições e relatório estatístico.

## 5. Avaliação retrospectiva do espaço COMPLETO, após pré-seleção

Executar separadamente, mantendo **HBench congelado**:

```bash
python -m methodology.tpcx_iot.stage91.cli verification \
  --reset --stations 2 --sensors 3 \
  --seed-steps 130 --bench-steps 500 \
  --read-operations 120 --read-threads 2 --repetitions 3 \
  --nodetool-container schemalens_tpcx_cassandra \
  --require-storage --require-full-audit \
  --output outputs/stage91_verification
```

Esse modo avalia as **nove composições condicionalmente completas** da etapa 8 mais o baseline externo (10 designs); nenhuma configuração descartada volta para HBench. Outputs:

- `81_stage91_verification_runs.json`
- `82_stage91_verification_summary.csv`
- `83_stage91_verification_vs_baseline.csv`
- `84_stage91_retrospective_quality.json`: preservação Top-1, interseção Top-3, near-best <= 5%, regret do p95 e retenção de Pareto **em 2D** (p95 ↓, ingestão ↑), usando a mesma carga e escala.

O 44,44% é **redução de deployments** (nove para cinco), não melhoria de latência. Com uma carga, uma escala e uma métrica p95, Top-k significa melhor *design completo medido*, não necessariamente Top-k de todos os possíveis workloads ou sistemas.

## 6. Experimentos maiores e limitações

- Use escala incremental, p.ex. `--stations 10 --sensors 20 --seed-steps 130 --bench-steps 500`, mas isso gera `126000` registros por tabela. Para manter a auditoria integral, aumente `--max-full-audit-events 126000` e reserve tempo/memória suficientes. Caso contrário, o resultado será `SKIPPED_EXCEEDS_LIMIT` e nunca deve ser descrito como auditoria de todos os registros.
- `--nodetool-container` captura bytes reais de SSTables após flush; esses bytes são sensíveis a compactação e ao ciclo de execução. Considere SSD, cache, memtables e métricas do hardware ao interpretar.
- A carga de consultas é sincronizada por **progresso de ingestão**, não por chegadas em tempo real a uma taxa fixa de QPS. Assim, os deployments têm a mesma quantidade e parâmetros de consultas, mas podem ter durações totais distintas.
- A fila de escrita SQLite é incluída no tempo reportado e pode influenciar a ingestão. O baseline passa pelo mesmo mecanismo. Isso não representa isoladamente a capacidade máxima de Cassandra.
- Leitura de dois intervalos de cinco segundos e sensores de uma subestação seguem o **perfil analítico congelado com sensor_name apenas**. Antes de reivindicar comportamento fiel ao TPC, inspecione o binding Java do kit 2.1.2.
- `IoTps` oficial permanece `null`. Sem kit, driver original, dois runs oficiais, replication >= 2 e validação oficial, não reivindique conformidade.
- `M3` foi ativado nas etapas anteriores, mas as tabelas atuais não variam a estratégia Cassandra de compaction; isso é um limite explícito deste experimento.

## 7. Questão científica e relação com o DKE

O método do SchemaLens **reduz o conjunto de alternativas que serão medidas**, não prevê um único vencedor. O artigo original diferencia **HBench** (seleção antes de benchmark) de **Htop** (Pareto medido em configurações completas). Por isso o baseline externo entra só na avaliação e as quatro alternativas excluídas só no modo `verification` retrospectivo. Consulte algoritmos 4 e 5 e métricas da seção 4.2 do DKE.

## 8. Procedência de resultados já obtidos

A primeira execução Stage9 (anterior, por você) encontrou H1 ~298,34 eventos/s e p95 ~3,693 ms, H4 ~178,83 eventos/s e p95 ~2,088 ms, mas foi a rodada **com leituras fixas**. Não compare quantitativamente esses valores contra as novas medições como se fossem o mesmo workload. O runner Stage9.1 não importa esses números para ativação, ranking ou HBench.
