# SchemaLens + TSBS: Etapa 6 — WritePlan e manutenção de séries/rollups

> **Extensão incremental**. Não modifica regras TS0–TS8/M1–M5, candidatos
> físicos da Etapa 4 ou QueryPlans da Etapa 5. Não executa C0–C9.

## 1. Objetivo científico

Nas etapas anteriores, ativamos famílias e produzimos nove representações
físicas Cassandra e 37 vínculos consulta/candidato. Agora precisamos definir
**como um único evento lógico de CPU alimenta cada representação**, inclusive
quando o evento é repetido, chega atrasado, é corrigido ou excluído.

O benchmark TSBS gera observações de CPU com `hostname`, timestamp e dez
métricas (`usage_user`, `usage_system`, ...). O TSBS Cassandra original separa
os campos por série, mas nossa unidade lógica de escrita é uma observação
**completa** `(hostname,timestamp_ns)` com todos os dez campos.

A Etapa 6 inclui uma implementação de referência em memória para testar
semântica e emitir `INSERT`/`DELETE` CQL parametrizados. **Não é um loader
Cassandra com recuperação de falhas, escalabilidade, concorrência ou garantia
transacional.** O executor experimental opcional é explicado abaixo.

## 2. Arquitetura: somente os componentes novos

```text
outputs/tsbs_stage4/10_cassandra_candidate_specs.json  ─┐
outputs/tsbs_stage5/15_query_plans.json                  ─┤
outputs/tsbs_stage4/14_stage4_manifest.json              ─┤── Stage6 planner
outputs/tsbs_stage5/17_stage5_manifest.json              ─┤      │
config/tsbs_stage6_write_policy.json                     ─┘      ▼
                                             outputs/tsbs_stage6/
                                               18_write_plans.json
                                               19_write_coverage_matrix.csv
                                               20_stage6_manifest.json
                                                       │
                               complete CPU events ────┤
                                                       ▼
                                         ReferenceMaterializer
                                           canonical event set
                                           physical row snapshots
                                           impacted-group index
                                           CQL UPSERT / DELETE
                                                       │
                                                       ├── Stage5 synthetic fetch/equivalence
                                                       │
                                                       └── optional experimental session adapter
``` 

O `planner.py` apenas lê outputs prévios. Ele **não reexecuta** ativação,
reescolhe bucket, seleciona vencedores por latência ou mexe em regras.

## 3. Quais transformações cada família recebe?

| Candidato (Etapa 4) | Unidade escrita | Estratégia |
|---|---|---|
| `ts0_metric_raw` | 10 linhas por observação | UPSERT raw por `(host,metric,time)` |
| `ts1_metric_bucket_1d` | 10 linhas | UPSERT raw com bucket UTC diário |
| `ts1_metric_bucket_7d` | 10 linhas | UPSERT raw com bucket UTC de sete dias |
| `ts2_wide_bucket_1d` | 1 linha | UPSERT com as dez métricas em conjunto |
| `ts5_max_by_host_minute` | até 10 grupos afetados | MAX exato por host/métrica/minuto |
| `ts5_max_by_host_hour` | até 10 grupos | MAX exato por host/métrica/hora |
| `ts5_sum_count_by_host_hour` | até 10 grupos | SUM e COUNT para AVG por host/métrica/hora |
| `ts5_global_max_minute` | 1 grupo | MAX global por minuto entre hosts |
| `ts6_hour_host_dimension` | 1 grupo | SUM e COUNT de todas as métricas por hora/host |

Uma observação nova, com nove candidatos materializados simultaneamente,
pode gerar **63 mutações CQL**: `10+10+10+1+10+10+10+1+1`.

**Isso é uma contagem para o catálogo completo, NÃO o custo real de um único
deployment escolhido na Etapa 8.** O materializador permite selecionar apenas
um subconjunto de candidatos via `candidate_ids`. Não contamos as nove tabelas
como um deployment obrigatório.

## 4. Por que não usamos Cassandra counters ou simples MAX incremental?

Exemplo: um host possui valores `15, 97` no mesmo minuto. O máximo é `97`.
Uma correção chega substituindo `97` por `12`.

A atualização ingênua `max(97,12)` preservaria **97 incorretamente**.
O materializador de referência mantém as observações canônicas e a associação
de cada registro aos grupos que alimenta. Quando alguma observação muda,
**recalcula o grupo afetado**: novo MAX `15`.

No caso de AVG, armazena `value_sum` e `sample_count` e não “média de médias”.
Repetir a mesma observação não incrementa o COUNT novamente. Se um valor
mudar, a soma é recalculada corretamente.

Implementação: `methodology/tsbs/stage6/materializer.py`, método `apply`.
São consideradas operações `upsert` e `delete` por identidade lógica
`(hostname,timestamp_ns)`, usando a política declarada:
`last_input_event_wins_for_same_host_and_timestamp`.

**Limite fundamental:** a memória contém eventos, resultados físicos e índices
de pertencimento. É uma implementação de **referência semântica** para pequena
escala. Não mantém estado durável, não lida com falha entre escrita bruta e
agregado, não faz coordenação distribuída e não é adequada para o volume total
do TSBS sem redesign (log de eventos, checkpoint, microbatch e reconciliação).

## 5. Exemplo prático

### Fonte: registro lógico

```python
observation = {
    "hostname": "host_0", "timestamp_ns": 0,
    "usage_user": 55.0, "usage_system": 12.0, "usage_idle": 20.0,
    # ... adicionar as outras sete métricas numéricas!
}
```

A validação exige as **dez métricas**, não aceita dados incompletos, NaN nem
infinito. Os testes usam a função `point()` para construir esses exemplos.

### TS2: CQL de escrita

```sql
INSERT INTO ts2_cpu_wide_1d (
    hostname, bucket_day, timestamp_ns,
    usage_user, usage_system, usage_idle, usage_nice, usage_iowait,
    usage_irq, usage_softirq, usage_steal, usage_guest, usage_guest_nice
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
```

O dia é calculado em UTC conforme a mesma convenção da Etapa 5.

### TS5: dados resumidos

```text
host_0, usage_user, 1970-01-01, minute_ns=0, max_value=55
```

Um evento atrasado ou corrigido provoca a recomputação apenas das chaves de
agregado afetadas; outras partições/grupos permanecem intactos.

## 6. Entrada TSBS real: pseudo-CSV Cassandra

O arquivo `tsbs_adapter.py` aceita o formato **cpu-only** documentado em
[docs/cassandra.md](https://github.com/timescale/tsbs/blob/master/docs/cassandra.md):

```text
series_double,cpu,hostname=host_0,region=eu-west-1,...,usage_user,2016-01-01,1451606400000000000,38.0
```

Ele reconstrói observações com dez métricas a partir das dez linhas do mesmo
`hostname,timestamp_ns`, valida o dia UTC e a consistência das tags estáticas.
A operação é **offline e em memória**: destinada a testes pequenos e não a
arquivos TSBS de gigabytes.

## 7. Instalação no repositório anterior

1. Extraia o ZIP do add-on **na raiz do projeto existente**.
2. Confirme que a raiz já contém `methodology/tsbs/stage4` e `stage5` e seus outputs.
3. Execute:

```bash
python3 -m methodology.tsbs.stage6.cli
python3 examples/show_stage6_writeplans.py
python3 -m unittest discover -s tests -v
```

O script `./run_tsbs_stage6.sh` equivale ao primeiro comando.

Somente estes outputs serão criados em `outputs/tsbs_stage6/`:

- `18_write_plans.json`: tabelas, tipos, PK/CK, `INSERT` e `DELETE` por candidato;
- `19_write_coverage_matrix.csv`: vincula cada QueryPlan a seu plano de escrita;
- `20_stage6_manifest.json`: evidências de integridade, contagens e limitações.

A documentação não atribui latência ou ganho de desempenho a esses testes.

## 8. Executor Cassandra opcional (avançado)

O arquivo `cassandra_executor.py` aceita uma sessão Cassandra com interface
`prepare`/`execute` (por exemplo, do pacote Python **cassandra-driver**), mas
nunca inicia conexão sozinho. Para execução real, é necessário criar as
tabelas com o DDL da Etapa 4 e apontar a sessão para um **keyspace isolado**.
O método exige `allow_experimental_writes=True`.

**Não é recomendado como loader de produção:** se houver erro durante uma
sequência de mutações, os dados brutos e agregados podem ficar divergentes.
Não existe journal/checkpoint durável, política de retry, isolamento de carga,
conflitos de concorrência ou procedimento automatizado de recuperação.
Não houve execução real em Cassandra neste pacote; o teste automatizado usa
uma **sessão simulada** para verificar os binds e o opt-in.

A integração de carga massiva, estado durável e manutenção transacional de
agregados é uma tarefa técnica posterior necessária antes do benchmark.

## 9. Relação com as etapas seguintes

A Etapa 6 fecha a lacuna entre *estrutura física* e *produção de dados* **no
nível de referência testável**, mas não prova a prontidão operacional de um
deployment completo.

- Etapa 7: custos e restrições (amplificação de escrita, tamanho das partições,
  frequência real de consultas e atualizações, custos de rollup).
- Etapa 8: consolidação de candidatos, compatibilidade, cobertura completa e
  HBench; apenas as representações escolhidas seriam mantidas.
- Etapa 9/10: carga Cassandra e benchmark real comparado ao baseline TSBS,
  conservando os mesmos dados, resultados lógicos e ambiente.

**Guarda metodológica:** não usar benchmark anterior para ajustar famílias,
parâmetros ou ranking antes de finalizar a seleção. Os valores de retenção,
TTL, TWCS e salt não são inventados; M2/M4/M5 não estavam ativados no cenário
TSBS inicial. M3 permanece evidência de append, não uma escolha automática
de compaction.
