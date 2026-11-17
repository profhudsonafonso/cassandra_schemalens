# SchemaLens + TPCx-IoT + Cassandra — Etapa 5: QueryPlans e cobertura condicionada

## 1. Finalidade científica

Esta etapa implementa, **sem alterar a ativação da etapa 3**, a noção do artigo DKE de que uma alternativa física só cobre uma consulta se existir um plano semanticamente equivalente, executável sob pré-condições verificáveis. Consulte a seção 3.7 / Algoritmo 4, `ValidEquivalentPlan(q,c)`; os QueryPlans ainda **não** constituem uma configuração de workload completa nem HBench.

O estágio anterior criou 5 specs para `TS0`, `TS1` (60 s e 3600 s) e `TS3` (60 s e 3600 s). Este estágio transforma seus SELECTs em planos com: (a) descoberta das partições, (b) geração dos parâmetros bind, (c) leitura de duas janelas, (d) mesclagem sem perda ou multiplicação de linhas, (e) pré-condições explícitas. Registra separadamente ingestão `Q_INGEST` (cujo WritePlan virá na etapa 6).

**O que não fazemos:** não editamos regras `TS0–TS8`/`M1–M5`, não ativamos TS5, não geramos WritePlans, não abrimos conexão com Cassandra, não medimos latência, não selecionamos HBench nem inferimos qual família vence.

## 2. Hipótese do perfil de dados (não verificada no kit oficial)

O arquivo congelado `Q_READ_2_WINDOWS.sql` filtra **somente `sensor_name`** e recupera `(event_ts, sensor_reading)` em duas janelas de **5 segundos inclusivas**. A especificação textual do TPCx-IoT 2.1.2 descreve as duas janelas, mas não resolve inequivocamente todos os detalhes do binding efetivo. Antes de reivindicar fidelidade ao kit oficial, confirmar a implementação das consultas no driver Java; literatura sobre o TPCx-IoT também descreve agregações e sensor de uma subestação específica.

Se for comprovado que o predicado inclui `driver_system_id`, não altere estes outputs manualmente: gere um **novo perfil versionado** desde o manifesto da carga, repita ativação, Stage4 e Stage5 e só depois execute experimentos. A comparação preserva as hipóteses e evita ajustar famílias após medir.

## 3. Estruturas e planos físicos

| Família | Candidato | PK | CK | Condição especial |
|---|---|---|---|---|
| TS0 | `ts0_series_raw` | `(driver_system_id,sensor_name)` | `(event_ts,event_id)` | catálogo completo de subestações |
| TS1 | `ts1_series_60s` | `(driver_system_id,sensor_name,bucket_start_ms)` | `(event_ts,event_id)` | catálogo + buckets de 60 s |
| TS1 | `ts1_series_3600s` | idem | idem | catálogo + buckets de 3600 s |
| TS3 | `ts3_sensor_60s` | `(sensor_name,bucket_start_ms)` | `(event_ts,driver_system_id,event_id)` | manter projeção adicional + buckets de 60 s |
| TS3 | `ts3_sensor_3600s` | idem | idem | manter projeção adicional + buckets de 3600 s |

O campo `event_id` é uma **decisão adicional do adaptador experimental**: mantém distintas as medições com igual timestamp e chaves lógicas, e precisa ser gerado estavelmente em retentativas de gravação. O dataset original não declara este campo.

### 3.1. Dois intervalos em Cassandra

Consulta lógica congelada:

```sql
SELECT r.event_ts, r.sensor_reading
FROM Reading r
WHERE r.sensor_name = :sensor_name
  AND ((r.event_ts BETWEEN :ti1_start AND :ti1_end)
    OR (r.event_ts BETWEEN :ti2_start AND :ti2_end));
```

Para cada janela o binder gera SELECTs preparados, sempre contendo igualdade para toda a PK e faixa da primeira CK (`event_ts`). Sem `ALLOW FILTERING` e sem `OR` em CQL. TS0/TS1 expandem uma requisição para **cada subestação** e cada bucket necessário; TS3 usa apenas `sensor_name` + bucket. A partir das respostas, o cliente junta os eventos, elimina somente leituras repetidas do **mesmo evento físico** que aparecem em duas janelas, e projeta `(event_ts,sensor_reading)`. Como o SQL **não** usa `DISTINCT`, duas medições diferentes com timestamp e valor iguais permanecem duas linhas.

Os limites são inclusivos. Um intervalo `59.000–64.000 ms` exige os buckets 0 e 60.000 ms (largura 60 s). Se terminar exatamente em 60.000 ms, **o bucket de 60.000 também é obrigatório**. O cálculo é `(ts_ms // width_ms) * width_ms`, com divisão piso inclusive para épocas negativas.

### 3.2. Pré-condições de cobertura

1. Para TS0/TS1, é necessário um catálogo **completo e atual** de subestações que podem produzir o nome de sensor consultado; o usuário do `bind_read_requests` deve informar `complete_station_roster=True` e `roster_provenance`. A flag é apenas **declaração do chamador**, não certificação do software.
2. TS3 deve receber uma linha equivalente para todo evento ingerido, com a mesma identidade e payload. Isso só será formalizado nos WritePlans da etapa 6.
3. É necessário ler **todas as páginas** de cada consulta Cassandra. Uma única página não equivale ao resultado SQL completo.
4. As duas janelas têm 5000 ms de largura e a janela histórica situa-se nos 1800 segundos anteriores ao início da recente, conforme o perfil de trabalho escolhido.
5. Devemos manter instantâneos coerentes quando se testa equivalência, evitando diferenças decorrentes de escritas concorrentes. Esta etapa não estabelece uma garantia de snapshot transacional entre múltiplas partições Cassandra.

**Status de cobertura:** `CONDITIONAL_OFFLINE_ONLY`. O plano parece estruturalmente possível; 25 verificações sintéticas com oráculo lógico passaram, mas ainda não há dados reais nem prova de plano executado em Cassandra. A escrita permanece `STAGE6_UNVERIFIED`, o benchmark `STAGE9_UNVERIFIED` e a elegibilidade para HBench completo permanece `false`.

## 4. Exemplo reproduzível sem Cassandra

Suponha dois sistemas `ST01` e `ST02`, e os intervalos:

- TI1 = `[59_000,64_000]` ms
- TI2 = `[0,5_000]` ms

Contagens de requisições CQL: TS0=4; TS1/60s=6; TS1/3600s=4; TS3/60s=3; TS3/3600s=2. São **quantidades de solicitações**, não resultados de desempenho; 60 s pode ser melhor ou pior empiricamente.

Execute após extrair o ZIP:

```bash
cd SchemaLens_TPCx_IoT_Cassandra_Stage5
python3 -m methodology.tpcx_iot.stage5.cli
python3 -m examples.show_stage5_queryplans
python3 -m unittest discover -s tests -v
# ou simplesmente:
./run_stage5.sh
```

Exemplo do bind para TS1:

```python
from methodology.tpcx_iot.stage4.candidates import ROOT, read_json
from methodology.tpcx_iot.stage5.runtime import ClosedWindow, bind_read_requests

plans = read_json(ROOT / 'outputs/tpcx_iot_stage5/20_queryplans.json')
plan = next(p for p in plans if p['candidate_id'] == 'ts1_series_60s')

requests = bind_read_requests(
    plan,
    sensor_name='temperature',
    recent=ClosedWindow(59_000, 64_000),
    historical=ClosedWindow(0, 5_000),
    stations=['ST01', 'ST02'],
    complete_station_roster=True,  # apenas declaração: seu loader deve verificá-la
    roster_provenance='example_two_station_fixture',
)
for request in requests:
    print(request['cql'], request['params'])
```

Os parâmetros da primeira requisição recente para TS1/60s são:

```text
['ST01', 'temperature', 0, 59000, 64000]
```

A segunda requisição para a mesma estação usa `bucket_start_ms=60000`. Essa enumeração não existe no SQL lógico; é responsabilidade do QueryPlan físico.

## 5. Artefatos gerados

| Arquivo | Papel |
|---|---|
| `20_queryplans.json` | Um QueryPlan por candidato, CQL e pré-condições |
| `21_read_coverage.json` | Cobertura estrutural, pré-condições e status de write/live |
| `22_offline_equivalence_probes.json` | 5 cenários sintéticos × 5 layouts = 25 comparações |
| `23_queryplan_manifest.json` | Hashes, flags de auditoria, contagens e limitações |

As cinco amostras verificam múltiplas subestações, sensores distintos, medições iguais com identidades diferentes, mudança de bucket (60 s), janelas que se tocam em um timestamp, épocas negativas e janelas vazias. O oráculo lógico lê a coleção de eventos original e implementa diretamente `sensor_name AND (janela 1 OR janela 2)`; a implementação candidata lê uma representação indexada por PK (simulação local), aplica filtros e faz a mesclagem. A checagem usa multiconjuntos dos resultados, não `set`, para preservar duplicatas legítimas.

## 6. Relação com o algoritmo original e próxima etapa

- Do Algoritmo 1 do artigo DKE: usamos **somente** candidatos concretos ativados e instanciados nas etapas anteriores.
- Do Algoritmo 4: implementamos um verificador **condicional de planos de leitura**, sem ainda declarar `cover(c)` comprovado em dados implantados.
- Do Algoritmo 5: nenhuma configuração HBench foi constituída. A cobertura do workload completo exige os próximos estágios e restrições de manutenção.

**Etapa 6:** gerar WritePlans para `Q_INGEST`, incluindo `event_id` idempotente, bucketização UTC, fanout de TS3, atualização/validação do catálogo de subestações, tratamento de falhas parciais, consistência, custos de manutenção e testes offline. Somente depois disso iniciar Stage7/8/9.

O objetivo desta etapa é **auditabilidade e equivalência lógica condicionada**, não demonstrar que Cassandra ou uma família time-series tem desempenho superior.
