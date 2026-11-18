# SchemaLens Cassandra Rev3 — S1: experimento controlado de janelas temporais

## 1. Objetivo e delimitação científica

**Pergunta S1:** com exatamente os mesmos dados físicos da campanha Rev3 B, como o intervalo consultado afeta latência, número de chamadas CQL e linhas iteradas em **TS2-U** (sem buckets) versus **TS2-D** (bucket diário)?

Esta é uma avaliação **exploratória pós-A/B**, motivada pelos resultados observados nessas campanhas. Não é uma nova confirmação independente e **não** modifica o HBench congelado. Os planos físicos `plan_id` previamente selecionados são reutilizados; os parâmetros temporais das instâncias Q2/Q7/Q9/Q12 são derivados deterministicamente. Esses novos parâmetros **não foram gerados diretamente pelo Java IoT Benchmark**; as classes e seus dispositivos/sensores vieram dos objetos originais da campanha B. Não é replay do SQL ou da mistura de frequências oficial.

A campanha B possui 54.000 células métricas, nove dispositivos e duas séries DOUBLE densas, com histórico de aproximadamente **20,826 dias**. O maior valor testado é 21 dias, que alcança aproximadamente todo o histórico, **não** demonstra uma avaliação de 30 dias ou partições próximas a 32 MiB.

### Matriz pré-definida

| Classe | 1 dia | 7 dias | 14 dias | 21 dias | Objetivo |
|---|---|---|---|---|---|
| `Q2` intervalo | sim | sim | sim | sim | Leitura temporal simples |
| `Q7` agregação por hora | sim | sim | sim | sim | Custo de agrupar mais registros |
| `Q9` intervalo descendente | sim | sim | sim | sim | Ordenação de tempo |
| `Q12` união de subconsultas | sim | sim | sim | sim | Fan-out entre dispositivos e buckets |
| `Q5` sem filtro temporal | controle | — | — | — | Controle histórico completo, **não** janela variável |
| `Q8` último valor | controle | — | — | — | Controle Q8, plano `DESC_SPARSE` |

São **18 casos × 3 variantes × 60 repetições = 3.240 latências**; cinco warmups por caso e variante. As três variantes são: `BASELINE_Q8_OPTIMIZED`, `R3H_38939db96444` (TS2-U) e `R3H_d2ea1974de53` (TS2-D). A TS0 não é incluída neste diagnóstico porque a hipótese é especificamente o compromisso *bucket* versus partição sem bucket; a avaliação Rev3 A/B já contém TS0.

As janelas usam **o mesmo início absoluto: 2024-01-01 00:00 UTC**, derivado do menor timestamp do próprio dataset. `Q12` atualiza **cada subconsulta**, não apenas o limite externo. Sensores, dispositivos, agregação e operações de conjunto permanecem os capturados pelo Java. Isso evita confundir a janela com variações na seleção de dispositivos ou seeds.

### Contagens descritivas que esperamos antes de medir latência

| Janela | Q2: chamadas TS2-U / TS2-D | Q12: chamadas TS2-U / TS2-D |
|---|---|---|
| 1 dia | 1 / 1 | 2 / 2 |
| 7 dias | 1 / 7 | 2 / 14 |
| 14 dias | 1 / 14 | 2 / 28 |
| 21 dias | 1 / 21 | 2 / 42 |

Essas contagens são derivadas de operações planejadas e das partições do dataset B. **Não são contadores do Cassandra no servidor, nem resultados de latência.** A execução vai medi-las no executor Python e comparar com contagens exatas derivadas de timestamps. Para agregações/uniões, a saída lógica pode ter cardinalidade muito menor que as linhas iteradas.

## 2. Pré-requisitos

- Ambiente virtual Python original com `cassandra-driver`.
- Na raiz `SchemaLens_IoT_Cassandra_V3_Automatico`, as pastas instaladas `SchemaLens_Rev3_Independent_Confirmatory`, `SchemaLens_V4_Rev3_PlanAware` e `SchemaLens_V4_Rev2_Confirmatory`.
- Workspace da **campanha B concluída**: `/home/hudson/Documents/schemalens_cassandra/schemalens_rev3_new_java_B_20261105_193618` com `HOLDOUT_GENERATION_OUTPUT.json` e as saídas Rev3.
- Keyspaces Cassandra B preservados e acessíveis na porta `9043`, **sem novas escritas**. A campanha B já foi colocada em SSTables via `nodetool flush`. Não altere compactação, schema, consistência, nem rode outra carga concorrente durante o S1.

**Segurança:** o programa usa os três keyspaces B exatos encontrados no manifesto verificado. Não executa `DROP`, `CREATE`, `TRUNCATE`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `flush` ou `compact`. Não executa benchmark A/B novamente. A preparação, verificação e exportação não acessam Cassandra.

## 3. Executar no Ubuntu / VSCode

Na raiz do projeto:

```bash
cd ~/Documents/schemalens_cassandra/SchemaLens_IoT_Cassandra_V3_Automatico
source ~/Documents/schemalens_cassandra/venv/bin/activate
unzip -o SchemaLens_Rev3_S1_WindowSweep.zip
S1="$(realpath SchemaLens_Rev3_S1_WindowSweep)"
export S1_B_WORKSPACE="/home/hudson/Documents/schemalens_cassandra/schemalens_rev3_new_java_B_20261105_193618"
bash "$S1/run_tests.sh"
```

Escolha **uma pasta inédita** de resultados, fora do experimento congelado:

```bash
S1_OUT="$(realpath -m "../schemalens_rev3_S1_B_$(date +%Y%m%d_%H%M%S)")"
echo "$S1_OUT"
```

**Primeiro, pré-registrar os parâmetros e gerar os casos sem Cassandra:**

```bash
bash "$S1/run_s1.sh" prepare "$S1_OUT"
bash "$S1/run_s1.sh" verify "$S1_OUT"
```

O pré-registro fixa os 18 objetos derivados, 60 repetições, 5 warmups, a seed de execução `20261127`, paginação `16`, hashes de dados/consultas, identificadores HBench e keyspaces. Ele recusa sobrescrever um diretório existente. Se houver qualquer erro na preparação, **não execute `run`**.

**Depois, medir somente consultas `SELECT` sobre B**:

```bash
bash "$S1/run_s1.sh" run "$S1_OUT"
```

A execução valida todas as **54 combinações caso/variante contra o oráculo lógico antes de medir**, verifica contagens de chamadas e linhas, aquece cada caso, sorteia a ordem dos casos a cada repetição e a ordem das três variantes dentro de cada caso, e verifica o oráculo em todas as observações medidas. Não inclui `set_keyspace` no cronômetro de cada consulta. Qualquer erro produz `results/failure_manifest.json` e preserva evidência parcial; **não repita sobre a mesma pasta**.

**Auditar os resultados de forma independente dos arquivos de resumo:**

```bash
bash "$S1/run_s1.sh" audit "$S1_OUT"
```

Esse comando reconstrói p95/p99/medianas, verifica 3.240 medições, 1.080 ordens de execução e todas as cardinalidades. Salva `results/AUDITORIA_S1_PT.md` sem alterar os resultados de latência. **Se falhar, envie o erro e não empacote como campanha validada.**

**Empacotar**:

```bash
bash "$S1/run_s1.sh" pack "$S1_OUT"
ls -lh rev3_S1_window_sweep_results.zip
unzip -t rev3_S1_window_sweep_results.zip
```

Envie `rev3_S1_window_sweep_results.zip` para a auditoria.

Caso o terminal seja fechado, reexporte `S1_B_WORKSPACE` com o mesmo valor e defina `S1_OUT` para **a pasta exata já criada**. Nunca gere outra pasta se a intenção for finalizar o pré-registro existente.

## 4. Arquivos produzidos

```text
schemalens_rev3_S1_B_YYYYMMDD_HHMMSS/
  s1_preregister.json          # pré-medida: política, hashes, seed, matriz
  s1_cases.json                # consultas derivadas e controles, com hash
  results/
    s1_raw_latencies.csv       # cada amostra, requisições, linhas, plano, oráculo
    s1_orders.csv              # ordenação de cada repetição
    s1_query_p95.csv           # p95/p99 por caso e variante, nearest-rank
    s1_window_comparison.csv   # comparação temporal TS2-U vs TS2-D e baseline
    manifest.json              # hashes pós-execução + proveniência
    AUDITORIA_S1_PT.md         # auditoria e percentis recalculados
```

Não apresenta os valores como p95 de um workload com frequências reais: são **p95 por caso**, mais comparação por consulta e janela. As linhas `iterated_rows` representam o que o executor consumiu, não bytes ou blocos lidos internamente pelo Cassandra. O estágio de verificação compara essas contagens com uma **estimativa descritiva derivada dos registros de B**; não é uma nova validação automática dos proxies congelados do Stage 7 para janelas que esse Stage 7 nunca recebeu.

## 5. Como interpretar e quais ameaças controlar

1. **Efeito da janela:** Q2/Q7/Q9/Q12 têm o mesmo instante inicial, dispositivos e sensores em todos os intervalos; o que muda é apenas o limite final.
2. **Efeito de bucket:** TS2-D pode abrir várias partições mesmo quando as linhas devolvidas são iguais; mostrar `execute_calls`, `iterated_rows` e p95 lado a lado.
3. **Efeito do estado do banco:** o S1 ocorre **após** o flush das campanhas A/B. Comparar diretamente p95 de S1 com o p95 **pré-flush** da campanha B seria metodologicamente inadequado.
4. **Cache, GC, driver, paging:** registrar estado do Cassandra, JVM e máquina; evitar jobs concorrentes e, idealmente, repetir o sweep em diferentes momentos com um **novo pré-registro** e sem confundir essas réplicas com amostras independentes de datasets.
5. **Duas séries densas:** não extrapolar para valores nulos, sensores esparsos ou tipos mistos. Os cálculos descritivos de linhas são explicitamente bloqueados se o dataset não for denso e alinhado.
6. **Não há novo HBench:** o `plan_id` continua o plano físico congelado B para cada Q. A validade semântica da variante com novos limites é comprovada pelo oráculo; os custos originais Stage 7 não são apresentados como predições para novas janelas.
7. **Não buscar um ponto de inversão artificial:** se TS2-D for pior nos quatro intervalos, esse é um resultado válido. O teste de partições muito maiores exige S2.

## 6. Segundo experimento: crescimento do histórico

O roteiro de S2, com matriz controlada por duração e densidade, está em [`S2_PLANO_PT.md`](S2_PLANO_PT.md). Ele é um pré-protocolo metodológico, **não** um executor pronto. Primeiro auditaremos S1, pois o S2 exigirá novas gerações Java e novas materializações Cassandra.
