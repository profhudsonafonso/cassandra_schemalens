# Etapa 7 — Custo analítico e viabilidade física de séries temporais no Cassandra

## 1. Escopo científico e ligação com o artigo DKE

Esta etapa implementa o equivalente de **estimativas pré-benchmark** e inspeções de viabilidade física para os candidatos produzidos pelas etapas 1–6. **Não** modifica o catálogo `TS0–TS8`, suas regras de ativação, o esquema EER, QueryPlans, WritePlans, nem configurações CQL da Etapa 4. Também não produz `HBench`, p95, IoTps estimado, ranking ou seleção de vencedor.

O artigo SchemaLens/DKE distingue a evidência EER/workload, a instanciação e consolidação de alternativas, a **pré-seleção estimada de configurações completas**, e a medição final de configurações **implantadas conjuntamente** (Seções 3.6, 3.7 e Algoritmos 4–5). A Etapa 7 é um insumo analítico não-ranking para as composições da Etapa 8: mede *proxies físicos dos candidatos individuais*, não latências e tampouco `Score_hat(h)` ponderado (as frequências de workload não estão disponíveis).

**Benchmark:** adaptação acadêmica de TPCx-IoT 2.1.2, baseada no SQL lógico congelado. A consulta de duas janelas usa o predicado por `sensor_name` e retorna registros individuais, mas ainda exige verificação contra o kit Java oficial; não existe conformidade ou certificação TPC alegada.

## 2. Proveniência, entradas e proteções

- `outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json`: tabelas, PK, CK e buckets imutáveis.
- `outputs/tpcx_iot_stage5/20_queryplans.json`: rotas de acesso e CQL congelados.
- `outputs/tpcx_iot_stage6/30_writeplans.json`: manutenção da ingestão por candidato.
- `config/tpcx_stage7_cost_policy.json`: **cenário hipotético** declarado antes de medir; inclui alertas estruturais, e proíbe ranking, HBench e uso de resultados experimentais.

O código verifica hashes e contratos internos das etapas anteriores. O CSV e JSON de saída incluem identificador físico para cada candidato, além de um manifesto com hashes SHA-256.

## 3. Cenário didático (não oficial)

10 subestações × 200 nomes de sensores × 24 horas × 1 registro/minuto por série = **2.880.000 eventos**. O evento completo é modelado com 1.024 bytes lógicos mais **64 bytes de overhead assumido** por linha e **128 bytes assumidos por partição**, com **fator de replicação 2**. São valores definidos para ilustrar os trade-offs e **não representam métricas reais de Cassandra** nem Scale Factor certificado de TPCx-IoT.

O caso assume todas as séries ativas e amostras no mesmo relógio UTC, no primeiro segundo de cada minuto, começando no instante Unix 0. Isso é um *cenário sintético*, não uma distribuição observada. O kit TPCx-IoT não fornece, nestes artefatos, taxa de amostragem por série, política de retenção, número de subestações para o teste real nem frequência relativa de consultas.

Duas janelas fechadas de cinco segundos (em segundos contados desde o início):

- Janela recente: `[7199, 7204]`, atravessa o segundo 7200 (limite de 60 s e de 3600 s).
- Janela histórica: `[6200, 6205]`, cobre outro intervalo de cinco segundos.

A diferença entre o começo da janela histórica e o fim da mais recente é menor que 1805 s (horizonte representativo do lookback de 1.800 s mais a janela); o par é escolhido didaticamente para testar as fronteiras do particionamento e **não substitui o gerador oficial de queries**.

## 4. Estimativa de volume por candidato

Seja `N_stations = 10`, `N_sensors = 200`, `H = 86400 s`, `Δt = 60 s`:

`N_events = N_stations × N_sensors × (H / Δt) = 2.880.000`.

Estimam-se partições pelo número de chaves físicas distintas:

- TS0: uma partição para `(driver_system_id, sensor_name)` ⇒ 2.000 partições, 1.440 linhas em cada uma **no horizonte finito**.
- TS1/60: `(driver_system_id, sensor_name, bucket_60s)` ⇒ 2.880.000 partições, 1 linha por partição.
- TS1/3600: `(driver_system_id, sensor_name, bucket_3600s)` ⇒ 48.000 partições, 60 linhas por partição.
- TS3/60: `(sensor_name, bucket_60s)` ⇒ 288.000 partições, 10 linhas por partição (cada nome de sensor é compartilhado entre dez subestações).
- TS3/3600: `(sensor_name, bucket_3600s)` ⇒ 4.800 partições, 600 linhas por partição.

As contagens consideram buckets de borda e deslocamento temporal usando aritmética inteira, sem enumerar milhões de eventos. Para pré-epoch negativos, o floor é preservado.

### Proxy de armazenamento

`row_bytes_proxy = logical_record_bytes + row_overhead_bytes_assumed = 1088 B`.

`storage_single_replica_proxy = events × row_bytes_proxy + partitions × partition_overhead_bytes_assumed`.

`storage_rf_proxy = storage_single_replica_proxy × replication_factor_assumed`.

O proxy **não representa tamanho SSTable**. Não modela compressão, tombstones, memtables, índice físico, compactação, bloom filters, I/O e retransmissões. A leitura CQL congelada seleciona inclusive `padding`; o proxy `expected_cql_selected_payload_bytes_proxy` inclui o registro, enquanto o resultado lógico final projeta apenas timestamp e valor (16 bytes hipotéticos por observação).

A projeção TS3 **não** deve ser comparada isoladamente com uma configuração que mantenha somente TS0/TS1: no desenho completo ela exige pelo menos uma fonte canônica com replay durável. Na Etapa 8, somam-se os espaços das tabelas selecionadas, descontando apenas duplicações efetivamente idênticas verificadas.

## 5. Custos de leitura (número de requisições, não latência)

O QueryPlan executa cada uma das **duas janelas** sem `OR` CQL, dividindo por bucket, e, nas famílias TS0/TS1, consulta todas as subestações. A fórmula é:

`requests = Σ_{window} (number_of_buckets_intersected × stations_fanout)`.

Para TS0 não há bucket temporal (uma partição por subestação/sensor), portanto `10 × 2 = 20` requisições. Na TS1, a janela recente corta o limite de buckets e gera `2 × 10`, e a histórica gera `1 × 10`, totalizando **30**. Na TS3, sem fanout de estações, são **3** requisições.

Com amostras homogêneas a cada 60 s, somente a janela recente contém uma observação por estação neste par didático, logo os cinco planos produzem **10 registros esperados**, sem inferir bytes realmente lidos nem tempo real de rede.

## 6. Custos de escrita e manutenção

Cada um dos cinco candidatos registra **1 INSERT por evento lógico se materializado isoladamente**. Entretanto, `TS3` é uma projeção materializada, e a implantação completa requer uma fonte canônica recuperável e TS3; a contagem **mínima** com uma fonte e uma projeção é **2 INSERTs por evento**. Essa contagem é um proxy de instruções CQL, não uma métrica de amplificação de escrita interna do Cassandra. Escrever todas as cinco tabelas na demonstração offline resultaria em cinco instruções por evento, sem que isso represente um deployment selecionado.

A infraestrutura de replay da Etapa 6 é **em memória**. Não existe garantia de atomicidade entre partições, recuperação durável ou exatamente uma entrega física. O código registra essas limitações como pré-condições, não as resolve silenciosamente.

## 7. Alertas analíticos, não regras de exclusão

- `TOTAL_PARTITION_COUNT_WARNING`: ultrapassou o limiar ilustrativo de 500.000 partições no cenário.
- `PARTITION_ROW_WARNING`: excedeu 100.000 linhas por partição (limiar didático).
- `PARTITION_BYTE_PROXY_WARNING`: o máximo aproximado ultrapassou 16 MiB/partição (limiar didático, **não limite fixo universal de Cassandra**).
- `QUERY_CQL_FANOUT_WARNING`: mais de 1.000 requisições por consulta lógica (limiar didático).
- `UNBOUNDED_PARTITION_OVER_TIME`: TS0 cresce sem bucket/TTL; é mantido como baseline.
- `COMPLETE_STATION_ROSTER_REQUIRED`: TS0 e TS1 requerem lista confiável de estações.
- `CANONICAL_SOURCE_REQUIRED`, `DURABLE_PROJECTION_REPAIR_REQUIRED`: TS3 precisa de fonte canônica e reconstrução.
- `OFFICIAL_KIT_PREDICATE_UNVERIFIED`, `NO_LIVE_CASSANDRA_VALIDATION`: restrições de validade comuns a todos.

Alertas **não são provas de inviabilidade nem exclusões automáticas de alternativas**. A viabilidade real deve ser verificada em Cassandra. M1 permanece apenas como escolha de bucket pré-fixada; M3 permanece ativado analiticamente **sem escolha automática de TWCS/STCS/LCS**. TTL também não é ativado.

## 8. Como reproduzir no VS Code (Ubuntu ou macOS)

Requisitos: Python 3.10+ e biblioteca padrão; não exige Docker, Java ou Cassandra nesta etapa.

```bash
cd SchemaLens_TPCx_IoT_Cassandra_Stage7
python3 -m methodology.tpcx_iot.stage7.cli
python3 -m examples.show_stage7_costs
python3 -m unittest discover -s tests -v
# Alternativa:
./run_stage7.sh
```

Os testes verificam determinismo, integridade dos arquivos anteriores, contagem de partições, fronteiras temporais UTC (inclusive antes de 1970), orçamento de requisições e rejeição de configuração que pretende usar resultados do benchmark para alterar ativação.

## 9. Outputs da etapa 7

- `40_scenario_evidence.json` — premissas do cenário sintético e seu hash.
- `41_physical_footprint_proxies.json` — linhas, partições, bytes lógicos e risco de crescimento.
- `42_read_cost_proxies.json` — requisições CQL, fanout e tamanho de resultado sintético.
- `43_write_cost_proxies.json` — número de comandos por evento, incluindo necessidade de manutenção da projeção TS3.
- `44_feasibility_diagnostics.json` — condições e alertas analíticos, sem excluir candidatos.
- `45_candidate_cost_matrix.csv` — tabela plana pronta para inspeção em planilha.
- `46_cost_manifest.json` — hashes e declarações explícitas sobre o que não foi executado.

## 10. Próxima etapa: consolidação (Algoritmos 4–5 do DKE)

A Etapa 8 deve deduplicar especificações físicas equivalentes, verificar os caminhos de leitura e escrita **para toda a carga**, construir composições coerentes e aplicar pré-seleção Pareto ou limitação de `k` com critérios fixados antes do benchmark. Não se pode afirmar que `TS3` cobre ingestão e leitura sem identificar a origem canônica e o plano completo de manutenção; nem que `TS0`/`TS1` cobre a consulta por nome sem roster completo verificado.

A observação do artigo de que `Score_hat(h)` pode usar estimativas de candidatos não autoriza apresentar esse valor como desempenho real. Somente a avaliação conjunta da etapa 9 poderá produzir resultados físicos da configuração completa.
