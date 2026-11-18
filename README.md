# SchemaLens no Apache Cassandra

O SchemaLens gera, **antes de qualquer medição**, um conjunto pequeno e explicável de projetos físicos candidatos
(o *HBench*) a partir de evidências do workload. Depois mede esse conjunto no Apache Cassandra sob um protocolo comum.
Este repositório reúne a metodologia e a avaliação em quatro benchmarks:

| Benchmark | Natureza | Pasta |
|---|---|---|
| IoT-Benchmark | séries temporais (IoTDB *table model*), 12 consultas | [`iot_benchmark/`](iot_benchmark/) |
| TPCx-IoT | ingestão concorrente + leitura de duas janelas | [`tpcx_iot/`](tpcx_iot/) |
| TSBS cpu-only | séries temporais (DevOps), 16 *templates* | [`tsbs/`](tsbs/) |
| RUBiS | leilão on-line (OLTP), 36 instruções, 4 cargas | [`rubis/`](rubis/) |

## Organização

```
README.md                  este arquivo
reproduce.sh               ponto de entrada único de reprodução (offline e com Cassandra)
setup_env.sh               cria ./.venv (Python 3.12) com as versões fixadas
requirements.txt           dependências diretas; requirements-lock.txt = lock completo
methodology/               artefatos de metodologia comuns aos quatro benchmarks
  protocolo_v1/            protocolo comum de execução v1 (protocol_v1.json + ambiente, estatística e métricas)
  unified_stage7_v3/       Stage 7/8 unificado (V3): estimador de trabalho físico, Pareto, seleção, freeze, testes
  docs/                    análise de consistência (PDF), docs da V2 e da V3 (método, auditoria, comparações, decisões)
  artigos/                 artigos Overleaf: v2_protocolo_v1/ e v3_unified/
<benchmark>/               iot_benchmark/, tpcx_iot/, tsbs/, rubis/
  README.md                o que é o benchmark, volumes de dados, resultados e como reproduzir
  original/                experimento original (RUBiS: v9_original/)
  v2_protocolo_v1/         V2: medição refeita sob o protocolo comum v1 (projeto congelado + harness + resultados)
  v3_unified/              V3: Stage 7/8 unificado (pré-benchmark, freeze, pré-registro, medição, análise)
```

Cada benchmark passou por três gerações, que ficam lado a lado na pasta dele:

1. **original**: o experimento como foi feito primeiro. Metodologia e harness próprios; para o RUBiS, a V9 com
   modelo de custo NNLS e ILP.
2. **V2, protocolo comum v1**: a [análise de consistência](methodology/docs/Analise_Consistencia_Metodologica_Benchmarks.pdf)
   mostrou que a fase de *medição* divergia entre os benchmarks. A V2 congela a fase de projeto de cada original e
   refaz a medição sob um protocolo único: imagem do Cassandra 4.1.12 fixada por digest, contêiner dedicado, 5
   repetições a frio, gate de flush, oráculo independente, bootstrap pareado, piso de ruído A/A, preservação/regret
   e ablações.
3. **V3, Stage 7/8 unificado**: a fase de *projeto* (Stage 7/8) ainda era diferente por benchmark. A V3 a substitui
   por um único módulo, com o mesmo vetor de trabalho físico, a mesma dominância de Pareto, a mesma viabilidade e a
   mesma diversidade estrutural. Não usa latência, fatores por família nem escore escalar. O HBench é congelado por
   SHA-256 e pré-registrado antes do Cassandra. Benchmarks cujo HBench não mudou reutilizam as medições V2 (IoT,
   TPCx); TSBS e RUBiS foram medidos de novo.

## Resultados em uma tabela

| | IoT-Benchmark | TPCx-IoT | TSBS cpu-only | RUBiS |
|---|---|---|---|---|
| Espaço V3 (deployments viáveis) | 3 | 9 | 24 | 256 |
| HBench V3 | 1 | 2 | 5 | 5 |
| Melhor medido preservado (V3) | sim, regret 0 | empatado pelo piso A/A, regret 0,17% | natural: empatado, 1,7%; alinhada: sim, 0 | contra baselines: V3H01 0,71–0,77× NoSE/QPG |
| Contra a referência | TS2-U é o melhor medido | TS3 ≈ 2,9 ms vs TS0 ≈ 4,7 ms | rank 1 V3 = 0,55× o esquema original do TSBS | mesmos vereditos em 4.1.12 e 3.11.11 |
| Medição V3 | reuso V2 (rotas idênticas) | reuso V2 (rotas idênticas) | nova: 26 variantes × 5 rep. | nova: 9 deployments × 4 cargas × 2 versões × 5 rep. |

Os detalhes, inclusive os pontos fracos (retenção de Pareto no TPCx, deployment lento admitido pela diversidade no
RUBiS, pisos A/A altos), estão em [`methodology/docs/v3/V3_RESULTS_COMPARISON.md`](methodology/docs/v3/V3_RESULTS_COMPARISON.md)
e nos artigos [V2](methodology/artigos/v2_protocolo_v1/main.pdf) e [V3](methodology/artigos/v3_unified/main.pdf).

## Reprodução

### Offline (sem Cassandra, cerca de 10 minutos)

```bash
./reproduce.sh setup      # ./.venv com as versões exatas de requirements-lock.txt (Python 3.12)
./reproduce.sh env        # Python, Docker e os digests das imagens do Cassandra
./reproduce.sh test       # 15 testes obrigatórios da V3 (41 casos)
./reproduce.sh verify     # atestação de relocação + os 4 freezes V3 (reexecuta o Stage 7/8 e exige saídas idênticas)
./reproduce.sh analysis   # refaz as análises V2 e V3 a partir das latências brutas versionadas e compara
./reproduce.sh articles   # regenera tabelas e figuras e compila os dois artigos (requer pdflatex)
```

`analysis` reexecuta cada `analyze.py`/`ablation.py` da V2 em uma cópia temporária e a análise V3 completa. Em
seguida compara cada CSV/JSON com o versionado (tolerância relativa de 1e-9). Neste repositório, todos os arquivos de
análise dos 4 benchmarks, nas duas versões, se reproduzem. Os do RUBiS só são refeitos quando os traces brutos estão
presentes (ver abaixo); sem eles, o passo é pulado e avisado.

### Com Cassandra (horas)

Requisitos: Docker, ≥ 24 CPUs lógicas (servidor em cpuset 0–3, cliente 8–11, análise 16–23) e ≥ 16 GB livres.
Imagens fixadas: `cassandra@sha256:25f6a4d5…` (4.1.12) e, só para o braço secundário do RUBiS,
`cassandra@sha256:5e13c93d…` (3.11.11).

```bash
./reproduce.sh v2 iot        # ~1 h     (iot_benchmark/v2_protocolo_v1/run_all.sh all)
./reproduce.sh v2 tpcx_iot   # ~1 h
./reproduce.sh v2 tsbs       # ~45 min  (inclui a geração de dados com o gerador Go oficial do TSBS)
./reproduce.sh v2 rubis      # ~4 h
./reproduce.sh v3            # TSBS (~1,5 h) e RUBiS (~3 h) em segundo plano; acompanhar com methodology/run_v3_unified.sh --status
```

Os harnesses só *criam* recursos: contêineres e keyspaces novos, nada é apagado. Eles recusam sobrescrever um
diretório de resultados existente. Para remedir, passe outro `--out`/`RUN`.

## Integridade e proveniência

* **Firewall:** a seleção (Stage 7/8) roda sob um interceptador de leitura de arquivos que aborta se ela tentar ler
  resultados medidos. Os manifestos registram cada arquivo aberto.
* **Freeze:** cada `<benchmark>/v3_unified/prebenchmark/FROZEN.sha256` guarda o SHA-256 do código, das entradas, das
  saídas, do cenário, do protocolo e do harness, gravado *antes* da medição. Todo ponto de entrada do Stage 9 chama
  `verify()` e aborta se algum hash mudar.
* **Relocação (D-V3-016):** os freezes foram gravados na organização antiga das pastas e continuam intactos.
  [`methodology/unified_stage7_v3/RELOCATION_ATTESTATION.json`](methodology/unified_stage7_v3/RELOCATION_ATTESTATION.json)
  prova três coisas:
  * cada arquivo congelado tem o mesmo SHA-256 no novo caminho;
  * o código mudou só nos caminhos (diff completo em `RELOCATION_CODE.diff`);
  * o Stage 7/8 reexecutado reproduz as saídas congeladas byte a byte.

  `./reproduce.sh verify` refaz essa prova.
* **Decisões:** [`methodology/docs/v3/V3_DECISION_LOG.md`](methodology/docs/v3/V3_DECISION_LOG.md), com cada decisão
  datada antes ou depois da medição.

### O que não está no git

| Conteúdo | Tamanho | Como obter / verificar |
|---|---|---|
| Traces brutos do RUBiS V2 e V3 (`rubis/*/…/raw/*.jsonl.gz`) | ~2,2 GB cada | SHA-256 em `rubis/v3_unified/benchmark/run1/RAW_SHA256.json`; regenerados por uma nova medição |
| Traces brutos da V9 original (`rubis/v9_original/runs/**/raw/`) | ~2,2 GB | evidência histórica da V9 |
| Datasets gerados do IoT original (`iot_benchmark/original/experiments/**/*.csv`) | ~260 MB | regeneráveis pelas sementes; hashes nos manifestos |
| *Ledgers* SQLite da ingestão TPCx (`outboxes/`) | ~350 MB | transitórios |

As latências brutas de IoT, TPCx e TSBS (V2 e V3), os resumos do RUBiS e todas as entradas congeladas, incluindo a
coorte do RUBiS e a referência do oráculo, estão versionados.

## Ramos

* `main`: esta organização (proposta no ramo `reorg/estrutura-reprodutivel`).
* Ramo de backup com a organização anterior: ver o histórico do git. Os commits `29986e1…fcd99bf` contêm a V2 e a
  V3 na estrutura antiga (`versao_ajustada/`, `outputs_v3/`, `unified_stage7_v3/` na raiz).
