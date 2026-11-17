# SchemaLens / TSBS — Etapa 5: QueryPlan e cobertura condicionada (TSDB only)

## 1. Relação com a metodologia original

A Etapa 3 ativa exclusivamente famílias TS0–TS8 e modificadores M1–M5.
A Etapa 4 já materializou nove **alternativas de estrutura** (PK/CK e CQL).
Esta Etapa 5 não reexecuta o motor de regras e não usa latências ou resultados
publicados do benchmark. Ela constrói planos de leitura *por par consulta ×
candidato*; não consolida implantações.

**Conceitos diferentes:**
- **Família ativada:** alternativa estrutural justificada pelas evidências.
- **CassandraCandidateSpec:** formato físico, tabela, PK/CK, CQL (Etapa 4).
- **QueryPlan:** como uma consulta recupera todas as partições necessárias,
  organiza resultados e computa sua resposta a partir de uma tabela candidata.
- **Cobertura comprovada:** exigiria também dados fisicamente carregados,
  executor Cassandra e validação de respostas num ambiente real. NÃO ocorre aqui.
- **WritePlan:** gera e mantém de modo correto as estruturas derivadas; Etapa 6.

## 2. Entradas (não alteradas)

```text
outputs/tsbs_stage4/10_cassandra_candidate_specs.json
outputs/tsbs_stage4/11_query_candidate_map.json
outputs/tsbs_stage4/12_query_read_primitives.json
outputs/tsbs_stage4/14_stage4_manifest.json
inputs/workload/query_manifest.csv
inputs/workload/sql/*.sql
inputs/schema/tsbs_cpu_only_schema.json
config/tsbs_stage4_physical_policy.json
```

A implementação valida a proveniência do manifesto da Etapa 4, igualdade de
candidatos/primitivas, 16 IDs de consultas e a semântica de cada SQL lógico no
subconjunto controlado (MAX/AVG, 1m/1h, high CPU, última leitura por host).
As regras Cassandra gerais C0–C9 não são sequer carregadas.

## 3. Operadores físicos e processamento no cliente

| Classe de consulta | Recuperação física | Operações de cliente |
|---|---|---|
| `single-groupby` | Uma ou mais séries/partições, ou rollup MAX/minuto | Mesclar máximos por minuto entre hosts; ordenar ascendente |
| `cpu-max-all` | Séries ou rollup MAX/hora | Mesclar máximos por hora de cada métrica entre hosts |
| `double-groupby` | Raw, SUM/COUNT por série, ou TS6 por hora+host | Calcular AVG = SUM/COUNT por métrica, host e hora |
| `high-cpu-*` | TS0/TS1 (10 séries) ou TS2 (uma linha larga) | Reconstituir dez métricas/timestamp; filtrar `usage_user > 90` no cliente |
| `lastpoint` | TS0 DESC, `LIMIT 1` por host e por métrica | Reconstituir a última observação de CADA host, NÃO a última global |
| `groupby-orderby-limit` | Raw completo antes do fim; ou rollup MAX global/minuto | Ordenar minutos decrescentemente e retornar cinco buckets |

### Exemplo Q13 — high-cpu-1

Para um host e uma janela de uma hora contida em um dia:
- TS0: 10 CQL reads (`hostname`, `metric_name`, range)
- TS1 bucket 1d: 10 CQL reads (`hostname`, `metric_name`, `bucket_day`, range)
- TS1 bucket 7d: 10 CQL reads (bucket de sete dias, ancorado na época Unix)
- TS2 wide 1d: 1 CQL read (`hostname`, `bucket_day`, range)

O número de leituras NÃO é latência. A consulta TS2 ainda filtra `usage_user`
no cliente; a etapa não introduz `ALLOW FILTERING` ou índice automaticamente.

### Exemplo Q10 — double-groupby-1

- TS0: consultar a métrica por host, mesclar por `hostname` e hora, calcular AVG.
- TS5: buscar `value_sum`/`sample_count` de uma métrica/host/hora.
- TS6: buscar uma partição por hora com `hostname` como clustering.

SUM/COUNT preserva a ponderação correta; usar `AVG(AVG)` seria incorreto
para janelas com quantidades de amostras diferentes.

### Exemplo Q15 — lastpoint

TS4 **não foi ativada**. O plano usa o candidato TS0, uma busca DESC LIMIT 1
por host e métrica. Isso exige o pressuposto testável de que os dez campos
possuem o mesmo último timestamp por host no dataset `cpu-only` do TSBS.
A implementação rejeita pivôs incompletos em testes sintéticos.

## 4. Condições de cobertura (essenciais)

### 4.1 Janelas abertas e alinhamento

As consultas TSBS filtram `[start_ns, end_ns)`. Uma agregação materializada por
minuto/hora não é necessariamente equivalente se `start_ns` ou `end_ns`
caírem no INTERIOR da janela de agregação: o valor pré-calculado inclui pontos
fora do intervalo lógico. Por isso, os 13 planos TS5 e os três TS6 são
**condicionais** a fronteiras alinhadas. `bind_read_requests()` interrompe o
processamento quando a condição falha.

Exemplo: Q01 janela `10:00:30–11:00:30` não pode utilizar diretamente MAX/minuto;
Q01 janela `10:00:00–11:00:00` pode, desde que o agregado esteja correto.
No cenário TSBS padrão, NÃO assumimos que janelas aleatórias serão alinhadas.
A Etapa 6 deve permitir combinar rollup de buckets completos com leituras
brutas dos limites (ou registrar ausência de cobertura).

### 4.2 Identificadores completos de hosts

`high-cpu-all`, `double-groupby`, `lastpoint` e `groupby-orderby-limit` exigem
o conjunto EXATO de hosts que aparece no banco carregado. Como não existe
um catálogo de hosts na Etapa 4, ele deve ser passado explicitamente pelo
usuário/loader. `complete_host_roster=True` é uma asserção do chamador, **não**
uma prova fornecida pela Etapa 5. O WritePlan deverá produzir/validar esse
catálogo antes de qualquer avaliação real.

### 4.3 Observações densas

As leituras escalares de dez séries para `high-cpu`/`lastpoint` precisam
reconstituir observações correspondentes. Os testes assumem o padrão do
TSBS `cpu-only` de emitir as dez métricas em cada observação; essa premissa
não vale para séries esparsas sem política adicional.

### 4.4 Buckets físicos

TS1 `7d` usa `bucket_day` UTC ancorado em 1970-01-01, com cada início em
múltiplos de sete dias desde essa data. O mesmo cálculo **deve** ser usado no
loader da Etapa 6. Os rollups TS5/TS6 mantêm buckets de armazenamento diários,
e sua resolução lógica é minuto ou hora.

### 4.5 Retenção e consistência

M2 (TTL), M3 (compactação), M4 (shard) e M5 (dados atrasados) não são
instanciados aqui. Em especial, um MAX/SUM/COUNT não é automaticamente
correto apenas porque sua tabela existe: requer WritePlan idempotente,
tratamento de duplicações e política de updates/backfill.

## 5. Saídas

```text
outputs/tsbs_stage5/15_query_plans.json
outputs/tsbs_stage5/16_coverage_matrix.csv
outputs/tsbs_stage5/17_stage5_manifest.json
```

Em nossa reprodução: **16 consultas**, **9 esquemas físicos** e **37 planos**;
**16 planos** demandam alinhamento temporal. Todas as linhas de cobertura são
`STRUCTURAL_WITH_RUNTIME_GUARDS`. Nenhuma indica execução real no Cassandra.

### Demonstração reprodutível no VS Code

```bash
python3 -m methodology.tsbs.stage5.cli
python3 examples/show_stage5_queryplans.py
python3 -m unittest discover -s tests -v
```

Os testes sintéticos verificam equivalência de MAX/minuto TS0↔TS5,
AVG/hora TS0↔TS5↔TS6, leitura high-cpu TS0↔TS2, último valor por host,
e último MAX global de cinco minutos TS0↔TS5; **não** são benchmarks.

## 6. Próxima etapa (Stage 6)

Implementar `WritePlan` e `SupportQuery` para as nove estruturas:
1. Ingerir cada observação nas métricas escalares/linha wide.
2. Definir como descobrir e manter a lista completa de hosts.
3. Manter MAX, SUM/COUNT, inclusive nas janelas atrasadas, sem erros de dupla contagem.
4. Adotar fallback para limites temporais parciais (ou recusar os candidatos derivados).
5. Comparar respostas com referência raw no mesmo dataset de forma automatizada.

Somente após implementação e validação física será possível declarar
*executable coverage* do banco Cassandra. A posterior consolidação/HBench
precisa medir configurações completas e nunca usar benchmarks antigos para
influenciar a ativação ou seleção.
