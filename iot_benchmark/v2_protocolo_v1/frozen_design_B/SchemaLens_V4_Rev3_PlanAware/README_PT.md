# SchemaLens Cassandra — Rev3: Plan-Aware Selection (seleção consciente do plano físico)

## 0. Escopo e integridade científica

Este pacote é **uma implementação experimental aditiva** para a instalação da Rev2 no Ubuntu. Ele não edita os arquivos V3, V4 Rev1, V4 Rev2 nem os resultados E1–E5. Ele NÃO adiciona a família TS9. Na Rev3, distinguimos três objetos:

1. **Template activation (ativação de template):** o workload fornece evidência para instanciar um candidato físico, por exemplo TS2-U ativada por Q2.
2. **Cross-query coverage (cobertura cruzada entre consultas):** uma vez existente, a mesma TS2-U pode responder Q7, se o oráculo lógico verificar essa possibilidade.
3. **Executable query plan (plano físico executável):** para cada par candidato–consulta, pode haver vários planos distintos. O custo deve ser calculado para o plano que será realmente usado.

**O código foi escrito em inglês** e o guia está em português. Os experimentos anteriores mostraram que a Q8 na TS0 percorreu 3.000 linhas, contra 2 previstas pelo Stage 7 Rev2. O controle E5 demonstrou um plano `DESC_LIMIT1` muito mais eficiente **naquela carga**. Essa evidência motiva a *nova revisão*, mas **não pode ser reaplicada sobre a mesma carga como uma nova validação confirmatória independente**.

### Dependências

Na pasta raiz `SchemaLens_IoT_Cassandra_V3_Automatico`, mantenha lado a lado:

- `SchemaLens_V4_Rev2_Confirmatory/` (implementação Rev2 já instalada)
- `SchemaLens_V4_Rev3_PlanAware/` (este pacote)

No `venv`: Python >= 3.10, dependências atuais do SchemaLens e `cassandra-driver` apenas para o probe ao vivo. Sem instalação nova para testes e congelamento offline.

## 1. O que realmente mudou

| Stage | Rev2 | Rev3 implementada aqui |
|---|---|---|
| 4 | Candidatos TS0/TS2-U/TS2-D/TS4 são ativados conforme A1 | **Não modificado**; reutilizamos o catálogo Rev2, sem ativar família nova |
| 5 | Uma rota `Q8__ts0_iot_raw` indica basicamente candidato e consulta | **Catálogo de planos executáveis**, como `Q8__ts0_iot_raw__TS0_FULL_SCAN` e `Q8__ts0_iot_raw__TS0_DESC_LIMIT1` |
| 6 | Custos de manutenção/escrita já definidos | **Não modificado**; preserva os planos de escrita do parent Rev2 |
| 7 | Um proxy por par consulta–candidato | **Um proxy por plano**: `request_count_proxy`, `physical_rows_proxy`, `returned_bytes_proxy`, `extra_bucket_fanout_proxy`, `partition_pressure_proxy` |
| 8 | Pareto sobre esquemas e rotas genéricas | **Pareto sobre configurações com o plano escolhido para cada consulta**; diversidade canônica explicitamente etiquetada |
| 9 | Executor podia inferir estratégia da família (TS0 Q8 fazia scan completo) | **Dispatcher estrito**: só executa o `plan_id` presente no HBench Rev3 e no catálogo Stage5; valida resultados pelo oráculo |

### Exemplo com Q8

```text
TS0 física: PRIMARY KEY ((device_id, sensor_name), timestamp_ms)
            CLUSTERING ORDER BY (timestamp_ms DESC)

Q8 + TS0:
   plan_id: Q8__ts0_iot_raw__TS0_FULL_SCAN
   CQL: SELECT timestamp_ms,value_double FROM ts0_iot_raw
        WHERE device_id=? AND sensor_name=?
   estimated_rows: all stored rows in the two selected sensor partitions

Q8 + TS0:
   plan_id: Q8__ts0_iot_raw__TS0_DESC_LIMIT1
   CQL: SELECT timestamp_ms,value_double FROM ts0_iot_raw
        WHERE device_id=? AND sensor_name=? LIMIT 1
   estimated_rows: one per populated sensor partition
```

Os dois planos possuem **a mesma família, a mesma tabela e as mesmas chaves**. Somente o plano de leitura muda. Se aparecer uma linha inesperada com valor `NULL`, `DESC_LIMIT1` usa uma varredura de segurança da partição; esse fallback é contado. Em nossa carga TS0 anterior não havia linhas nulas materializadas.

Exemplo de *cross-query coverage* (cobertura cruzada): Q2 justifica materializar TS2-U, mas Q7 pode usar o candidato se o Stage5 Rev2 marcou a cobertura com `DIRECT`. Essa lógica **não é eliminada nem confundida** com a seleção de planos físicos.

### Outros planos considerados

| Representação | Consulta | Planos |
|---|---|---|
| TS0 | Q8 | `TS0_FULL_SCAN`, `TS0_DESC_LIMIT1` |
| TS0 | Outras | `TS0_RANGE_SCAN` (inclui operações irrestritas) |
| TS2-U | Q8 | `TS2U_FULL_SCAN`, `TS2U_DESC_SPARSE` |
| TS2-U | Outras | `TS2U_RANGE_SCAN` |
| TS2-D | Q8 | `TS2D_FULL_SCAN`, `TS2D_DESC_SPARSE` |
| TS2-D | Outras | `TS2D_BUCKET_SCAN` |
| TS4 | Q8 | `TS4_SNAPSHOT_LOOKUP` |

O Stage5 impõe uma pré-condição ao plano `TS0_DESC_LIMIT1`: é necessário verificar na **DDL congelada** que o timestamp esteja em ordem DESC. A cobertura lógica é herdada do oráculo Rev2; a validação do plano físico específico acontece no probe Stage9 (e futuramente em um novo holdout independente).

## 2. Stage 7: fórmula e correção da Q8

Mantemos os mesmos coeficientes declarados antes da campanha Rev2:

```text
C_read(plan) = 4 * request_count
             + 0.02 * physical_rows
             + 0.20 * (returned_bytes_proxy / 1024)
             + 2 * extra_bucket_fanout
             + 5 * partition_pressure
```

**Não ajustamos esses pesos** para tentar reproduzir os p95 do E5. A novidade é que cada plano produz um conjunto explícito de contagens.

- `TS0_FULL_SCAN`: estima **todas** as observações armazenadas nos sensores de Q8. Não mais `2` quando o executor percorre 3.000.
- `TS0_DESC_LIMIT1`: estima **uma linha** para cada partição de sensor que contém leitura (ou zero para partição vazia).
- `TS2U_FULL_SCAN`: todas as linhas timestamp dos dispositivos selecionados.
- `TS2U_DESC_SPARSE`: percorre timestamps descendentes até encontrar o último valor não nulo de cada sensor; reutiliza a previsão Rev2.
- `TS2D_BUCKET_SCAN`: continua a precificar chamadas e fan-out de buckets, inclusive quando um bucket do intervalo está vazio.

**Limite:** os proxies de bytes não representam tamanho real de SSTable compactada. A telemetria `execute_calls` e `iterated_rows` não mede acesso interno ou round-trips de paginação no servidor Cassandra. Isso já foi explicitado nas auditorias Rev2/E5.

## 3. Instalação e testes

Na raiz do projeto:

```bash
unzip -o SchemaLens_V4_Rev3_PlanAware.zip

REV3="$(realpath SchemaLens_V4_Rev3_PlanAware)"
REV2="$(realpath SchemaLens_V4_Rev2_Confirmatory)"

bash "$REV3/run_tests.sh"
```

Os testes são offline: não conectam Cassandra. Validam custos de Q8, diversidade, IDs distintos, DDL ASC recusada para `DESC_LIMIT1`, fallback de null, bloqueio de escrita e integridade do congelamento.

## 4. Congelar a Rev3 exploratória (não é nova confirmação)

Utilize o **mesmo experimento nativo já executado**. O novo congelamento é armazenado em pasta distinta e não modifica a Rev2.

```bash
WORK="$(realpath -m ../schemalens_confirmatorio_rev2)"
RUN="$WORK/java_work/runs/v3_20261026T200853Z_pid4169024"
NEW_EXP="$RUN/schemalens_rev2_ts5_contract_validated"
RESULTS="$NEW_EXP/outputs/v4_rev2_confirmatory/batch_15eadb0f"

bash "$REV3/run_rev3.sh" "$NEW_EXP"

bash "$REV3/run_rev3.sh" "$NEW_EXP" --verify-only
```

O comando valida os hashes dos arquivos Rev2/V3 e cria **somente**:

```text
$NEW_EXP/outputs/v4_rev3_planaware_prebenchmark/
  stage5/15_executable_plan_catalog.json
  stage5/16_executable_plan_catalog.csv
  stage7/23_per_plan_read_proxies.json
  stage7/25_cost_policy.json
  stage8/28_hbench_v4_rev3_frozen.json
  stage8/29_all_compatible_covers.json
  stage8/30_hbench_summary.csv
  stage8/31_plan_aware_routes.csv
  stage8/manifest.json
```

O congelamento falha se a pasta já existe; **não apague a evidência nem sobrescreva os hashes**. Ele verifica integridade do parent antes e depois, e usa `R3H_*` para não confundir os deploys históricos `R2H_*`.

## 5. Probe Stage 9 — validação física sem escrita

Depois do congelamento e somente se os keyspaces Rev2 ainda existirem:

```bash
OUT="$(realpath -m ../schemalens_rev3_probe_$(date +%Y%m%d_%H%M%S))"

bash "$REV3/run_probe.sh" "$NEW_EXP" \
  --results "$RESULTS" \
  --out "$OUT" \
  --host 127.0.0.1 \
  --port 9043 \
  --page-size 16
```

O probe Stage9 **não cria, apaga, faz flush ou insere linhas**. Ele faz consultas `SELECT` e só reutiliza keyspaces Rev2 quando a lista de tabelas do deployment Rev3 é **idêntica** à de algum deployment Rev2 já materializado. Se uma configuração Rev3 com TS4 não existir em Rev2, será ignorada, e isso não deve ser interpretado como sucesso na validação física dessa rota. Se nenhuma configuração for compatível, o programa falhará.

As saídas em `$OUT` são:

- `read_only_probe.csv`: deployment, Q1–Q12, `plan_id`, estratégia, chamadas `Session.execute`, linhas iteradas e status do oráculo.
- `manifest.json`: hashes dos HBenches Rev2/Rev3, quantidade de rotas, limitações e escopo de auditoria.

Para enviar:

```bash
cd "$(dirname "$OUT")"
zip -r rev3_planaware_probe_results.zip "$(basename "$OUT")"
```

## 6. O que este pacote ainda NÃO demonstra

- **Não é uma nova confirmação independente:** Rev3 foi projetada depois dos resultados de E5; o dataset atual é o mesmo já medido pela Rev2.
- Não contém um novo executor confirmatório destrutivo com `DROP/CREATE`. O Stage9 fornecido é um **probe read-only**; primeiro validamos os vínculos de plano na base materializada, sem riscos às evidências anteriores.
- Não corrige a ausência de SQL oficial ou frequências oficiais do IoT Benchmark. Mantemos um objeto Java por classe (Q1–Q12).
- Não resolve ainda concorrência de TS4, atualização atrasada, tombstones ou custos internos Cassandra.
- Não generaliza para todas as famílias TS0–TS8; a extensão foi construída sobre o catálogo e a cobertura Rev2 para TS0, TS2-U, TS2-D e TS4.

## 7. Passos seguintes — avaliação realmente confirmatória da Rev3

1. **Auditar o congelamento exploratório e a execução read-only**: verificar se Q8/TS0 utiliza o `DESC_LIMIT1` congelado e se os resultados correspondem ao oráculo Java. Confirmar todas as rotas existentes sem prometer p95 novo.
2. **Preparar outro holdout Java**: workspace distinto de Rev2 e V3, seeds e parâmetros de consulta novos, mais dispositivos e sensores, com múltiplos objetos por classe; resolver a Q1 sem resultados.
3. **Congelar primeiro o catálogo de planos e o HBench Rev3 nesse novo holdout**: mesmo código e coeficientes, nenhuma informação de latência/ingestão usada na seleção. Na nova geração, talvez seja preciso adaptar a captura de múltiplos objetos sem alterar o seletor depois de ver resultados.
4. **Criar o runner de confirmação Rev3 em keyspaces `sl_conf_r3_*` descartáveis**, materializando **somente** os deployments congelados, incluindo baseline otimizada e controles de planos quando aplicável. Registrar plano real, volume transferido, latência, armazenamento e operações de escrita.
5. **Executar em mais de um seed/campanha** para análise de variância entre gerações, cobrindo longa duração e sensores esparsos. Testar TS2-U versus TS2-D com diferentes comprimentos de partição e amplitudes temporais.
6. **Ablations (ablações)**: sem cobertura cruzada, com TS0 obrigatória, sem seleção de plano (`FULL_SCAN`), e Rev3 integral, mantendo mesma carga e comparando DSR, top-1, regret, p95 e espaço físico.

Essa sequência evita *data leakage* (vazamento de informação) e a alteração retrospectiva de um HBench já medido.
