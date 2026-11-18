# E3 — auditoria e interpretação científica

O ZIP E123 contém somente E3 (1, 7, 30 dias); E1 e E2 falharam antes das medições ao invocar `Cluster.refresh_schema_metadata()`.

## Auditoria dos dados enviados

- E3: 3 escalas × 5 configurações × 17 consultas × 30 rodadas = **7.650 observações brutas**.
- 255 grupos configuração/consulta/escala; 255 p95 recalculados e coincidentes com os CSVs.
- SHA-256 de HBench em todos os manifestos: `2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb`.
- 12 classes `NATIVE_FIXED_*` mantêm parâmetros do Java; cinco `EXTENDED_*` são sondas sintéticas.
- A escala de 1/7/30 dias replica deterministicamente as medições nativas em novos timestamps. Não é um novo dataset independente do IoT Benchmark nem replay oficial.

## Média dos p95 das 12 classes com parâmetros Java fixos (ms)

| Configuração | 1 dia | 7 dias | 30 dias |
|---|---:|---:|---:|
| Baseline | 1,762 | 4,188 | 9,801 |
| TS0+TS4 | 2,677 | 3,901 | 7,781 |
| TS0+TS2-U+TS4 | 2,229 | 3,430 | 8,066 |
| TS0+TS2-D+TS4 | 2,333 | 3,858 | 7,863 |
| TS0 | 2,793 | 6,615 | 15,218 |

Aos 7/30 dias, a média dos p95 aparenta favorecer HBench, **mas a mudança é quase toda associada a Q8**, última observação global (não é janela fixa).

## Q8 e efeito do full scan da baseline

| Dias | Baseline Q8 p95 | Melhor TS4 Q8 p95 | Linhas baseline / TS4 |
|---|---:|---:|---:|
| 1 | 1,592 | 0,721 | 300 / 2 |
| 7 | 18,985 | 1,653 | 2.100 / 2 |
| 30 | 54,388 | 1,365 | 9.000 / 2 |

**Limitação de justiça:** no executor atual a baseline recupera *todo* o histórico para Q8. Por usar clustering decrescente de timestamp, é necessário testar uma baseline otimizada com `LIMIT` e/ou leitura incremental que mantenha exatamente a semântica `last-by` para sensores com valores ausentes. A vantagem de TS4 medida frente a uma baseline de full scan não estabelece vantagem sobre o melhor plano possível da baseline.

## Média dos p95 das 11 consultas, removendo Q8 (ms)

| Configuração | 1 dia | 7 dias | 30 dias |
|---|---:|---:|---:|
| Baseline | 1,778 | 2,843 | 5,747 |
| TS0+TS4 | 2,846 | 4,097 | 8,365 |
| TS0+TS2-U+TS4 | 2,365 | 3,502 | 8,624 |
| TS0+TS2-D+TS4 | 2,479 | 4,058 | 8,379 |
| TS0 | 2,847 | 4,437 | 9,470 |

A baseline permanece mais rápida nas três escalas quando se exclui Q8. Isso reforça a prioridade dos diagnósticos E1/E2 (dividir tempo DB/cliente; comparar baseline e TS2-U com CQL idêntico).

## Sonda temporal de 30 dias (Q2 full span)

| Modelo | CQL requests | p95 (ms) |
|---|---:|---:|
| Baseline | 1 | 51,019 |
| TS2-U | 1 | 48,119 |
| TS2-D | 30 | 74,341 |

A estrutura TS2-D enumerou 30 buckets diários enquanto a TS2-U leu uma partição temporal por dispositivo. Não significa que TS2-D seja sempre inferior, pois partições maiores, concorrência, compactação e distribuição não foram avaliadas nesta escala.

## Limitações adicionais

- Os três testes de escala foram executados sequencialmente na mesma instância, sem repetições independentes por escala.
- O executor de Q8 da baseline é um potencial confundidor que deve ser otimizado em estudo separado.
- Os tempos incluem driver, transferência e transformação no Python, não somente processamento do servidor.
- A média não ponderada dos p95 individuais não é o p95 de uma mistura operacional com frequências reais do IoT Benchmark.
- Nenhuma medição de E1/E2 consta no ZIP enviado; não se deve atribuir sua falha à lentidão do banco.

Ver `e3_aggregate_audited.csv` para os valores sem arredondamento para três casas.
