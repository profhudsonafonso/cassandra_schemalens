# S3 — Auditoria final (campanha R1)

Gerado em 2026-11-10T21:01:52+00:00 por `s3_physical.py report` a partir de evidências auditadas.

## Status

- Pré-registro antes de qualquer medição: `s3_benchmark_preregister.json`.
- 216/216 validações semânticas PASS; traços de acesso idênticos aos do S2.
- 12.960 latências e 4.320 ordens auditadas; ordem aleatória idêntica à do S2 (pareamento).
- Gate de armazenamento antes da medição: todas as 12 tabelas com SSTable ≥ 1 e memtable vazia.

## Vitórias nas 16 comparações temporais por densidade

| Fator | S2 U/D | S3 U/D | Mediana D/U S2 | Mediana D/U S3 | Vencedor mudou |
|---:|---:|---:|---:|---:|---:|
| 1× | 14/2 | 11/5 | 1.330 | 1.282 | 5 |
| 2× | 12/4 | 12/4 | 1.236 | 1.167 | 2 |
| 4× | 15/1 | 13/3 | 1.166 | 1.119 | 2 |
| 8× | 13/3 | 12/4 | 1.065 | 1.095 | 1 |
| **Total** | **54/10** | **48/16** | | | **10** |

## Mediana p95 S3/S2 por variante (casos temporais)

| Fator | REF | TS2-U | TS2-D |
|---:|---:|---:|---:|
| 1× | 1.054 | 1.047 | 1.037 |
| 2× | 1.094 | 1.108 | 1.070 |
| 4× | 1.102 | 1.108 | 1.115 |
| 8× | 1.028 | 1.113 | 1.147 |

## Q12 em 21 dias (p95, ms)

| Fator | S2 U | S2 D | S3 U | S3 D | D/U S2 | D/U S3 |
|---:|---:|---:|---:|---:|---:|---:|
| 1× | 36.828 | 64.163 | 38.530 | 66.871 | 1.742 | 1.736 |
| 2× | 73.896 | 98.335 | 88.390 | 112.760 | 1.331 | 1.276 |
| 4× | 149.230 | 176.019 | 170.781 | 235.253 | 1.180 | 1.378 |
| 8× | 335.982 | 372.225 | 392.331 | 475.098 | 1.108 | 1.211 |

## Estado físico S3 antes da medição

| Fator | Variante | SSTables | Live bytes | Memtable cells | Partição média (B) | Partição máx (B) | Partições (est.) | Inalterado na medição |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 1× | REF | 1 | 377879 | 0 | 105778 | 105778 | 9 | sim |
| 1× | TS2-U | 1 | 377838 | 0 | 105778 | 105778 | 9 | sim |
| 1× | TS2-D | 1 | 386340 | 0 | 5662 | 5722 | 189 | sim |
| 2× | REF | 1 | 772090 | 0 | 219342 | 219342 | 9 | sim |
| 2× | TS2-U | 1 | 771728 | 0 | 219342 | 219342 | 9 | sim |
| 2× | TS2-D | 1 | 785540 | 0 | 9887 | 9887 | 189 | sim |
| 4× | REF | 1 | 1598246 | 0 | 454826 | 454826 | 9 | sim |
| 4× | TS2-U | 1 | 1598026 | 0 | 454826 | 454826 | 9 | sim |
| 4× | TS2-D | 1 | 1631575 | 0 | 20339 | 20501 | 189 | sim |
| 8× | REF | 1 | 3430635 | 0 | 943127 | 943127 | 9 | sim |
| 8× | TS2-U | 1 | 3430274 | 0 | 943127 | 943127 | 9 | sim |
| 8× | TS2-D | 1 | 3478050 | 0 | 42173 | 42510 | 189 | sim |

## Interpretação permitida

- Leitura descritiva de uma única réplica (R1). A diferença S3×S2 mistura o estado físico (SSTable vs memtable) com o momento da execução (05/11 vs 10/11) e com a carga do host; não é efeito causal isolado do flush.
- O page cache do SO provavelmente estava quente (SSTables recém-escritas): S3 mede "SSTable quente", não disco frio.
- Contagens de chamadas e linhas são do cliente Python, não I/O do servidor.
- Sem alegação de significância estatística; robustez exige réplicas independentes (r2, r3).
- Evidência adicional: o container parou de forma não limpa em 10/11 (exit 255); o replay do commitlog persistiu as memtables S2 em SSTables. As medições S2 continuam válidas para o estado em que foram feitas.
