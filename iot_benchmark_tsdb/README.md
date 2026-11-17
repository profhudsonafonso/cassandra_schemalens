# Execução automática da ponte oficial V3 (novo)

O procedimento manual do guia V3 agora pode ser executado em **um comando**:

```bash
./run_v3_auto.sh --check
./run_v3_auto.sh
```

A primeira execução requer Java 17+, Maven, Git, Python 3 e acesso à Internet. Os dados e as consultas são gerados pelo código Java upstream no commit fixo; em seguida, o SchemaLens executa os Stages 1–8 em uma cópia isolada, **sem Cassandra e sem benchmark**. Todos os resultados anteriores permanecem intocados.

[Guia automatizado com opções e estrutura de saída](docs/V3_AUTO_INTEGRACAO_PT.md).

---

# SchemaLens IoT/Cassandra — Official Query-Object Bridge V3

**Status: preparatório, Stages 4–8 offline; Stage 9 opcional e local. Não equivale ao benchmark oficial.**

Esta versão independente da Rev2 inclui:

- os 6 candidatos físicos e as variantes diárias/sem bucket de TS2;
- novo contrato semântico de Q3/Q5/Q6/Q10, Q7/Q11, Q8 e Q12 derivado da implementação Java no modo `table`;
- exportador `java_bridge/.../SchemaLensQueryCapture.java` que usa `GenerateQueryWorkLoad` oficial (arquivo **não compilado nem executado** neste ambiente);
- verificador JSONL `phase_a1/tools/official_query_capture.py` e nova opção `--official-query-objects` em `phase_a1/tools/run_a1.py`;
- `scripts/run_native_prebenchmark.sh` para dados e amostras Java gerados localmente;
- **41 testes Python aprovados** e verificação Stage 9 offline, sem Cassandra.

## Execução do fixture, no VS Code

```bash
cd SchemaLens_IoT_Cassandra_OfficialBridge_V3
bash run_prebenchmark.sh
```

## Execução com entradas reais do gerador Java

**Leia [`docs/V3_OFFICIAL_BRIDGE_PT.md`](docs/V3_OFFICIAL_BRIDGE_PT.md) antes.** Ele explica como instalar o exportador na cópia oficial do IoT Benchmark, gerar os dados e executar:

```bash
bash scripts/run_native_prebenchmark.sh /CAMINHO/DADOS_GERADOS \
  /CAMINHO/native_queries.jsonl /CAMINHO/config.properties 500000
```

Os manifestos congelados do Stage 8 continuam em `outputs/stage8/`. Não confunda os resultados incluídos no ZIP com resultados obtidos a partir de Java: os arquivos pré-gerados no ZIP são **somente do fixture ilustrativo A1**.

## Advertências

O pacote importa **uma amostra de objetos de parâmetros por classe**, não uma sequência real de SQL ou a carga probabilística multicliente. Os benchmarks anteriores NÃO são entradas. A compatibilidade final com o SQL IoTDB continua pendente; o Stage 9 em Cassandra só deve ser usado localmente e não representa uma pontuação oficial IoT Benchmark.
