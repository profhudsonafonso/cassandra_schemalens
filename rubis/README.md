# RUBiS no Cassandra

Site de leilão RUBiS: 36 instruções (28 leituras e 8 escritas) em quatro cargas, *browsing*, *bidding*, *update10x* e
*update100x*, com frequências da E05. O espaço de projeto é o catálogo C0–C9 da SchemaLens V9: tabelas-base,
tabelas por consulta, junções desnormalizadas, fusões e projeções. São 24 tabelas canônicas.

## Volume de dados (coorte medida)

Coorte congelada (`v2_protocolo_v1/frozen/cohort_source.pkl`, regenerada neste servidor por `rubis_v1/gen_data.py`):
**7 392 itens, 5 690 lances, 6 529 comentários, 5 300 compras, 21 855 usuários** (coorte + fechamento), 500 categorias e
50 regiões. Por deployment, repetição, versão e carga: 1 000 aquecimentos e 10 000 requisições medidas (histórias
semeadas da V9). A V9 original projetava sobre a escala completa (2 M itens, 20 M lances).

## Gerações

| Pasta | O que é | Entrada |
|---|---|---|
| [`v9_original/`](v9_original/) | SchemaLens V9: pipeline E1–E23, catálogo C0–C8, síntese de rotas, **modelo de custo NNLS + ILP** (H01–H05), medição E20 original | [`v9_original/README_V9.md`](v9_original/README_V9.md), `v9_original/docs/` |
| [`v2_protocolo_v1/`](v2_protocolo_v1/) | E20 refeito sob o protocolo comum v1 em **Cassandra 4.1.12 e 3.11.11** (mesmos dados, braços ABBA) | [`v2_protocolo_v1/README.md`](v2_protocolo_v1/README.md) |
| [`v3_unified/`](v3_unified/) | Stage 7/8 unificado: enumeração exata de 2^15 subconjuntos, 256 viáveis, sem NNLS; **nova campanha** | abaixo |

## Resultados

**V2** (H01–H05 da V9 + NoSE + QPG + V8-norm + A/A; 4.1.12): H01 / NoSE = 0,75 (browsing), 0,66 (bidding), 0,72
(update10x); H01 / QPG = 0,64; 0,67; 0,74. Em update100x há empate. Os vereditos são os mesmos em 3.11.11.

**V3** (HBench V3 = V3H01..V3H05; V3H01/V3H03/V3H04 = H01/H02/H04 da V9 com rotas idênticas; V3H02 e V3H05 inéditos):

| 4.1.12 | browsing | bidding | update10x | update100x |
|---|---|---|---|---|
| V3H01 / NoSE | 0,728 ↓ | 0,722 ↓ | 0,713 ↓ | 0,971 = |
| V3H01 / QPG | 0,769 ↓ | 0,763 ↓ | 0,757 ↓ | 1,041 = |
| melhor dentro do HBench | V3H01 | V3H01 | V3H01 | V3H04 (todos empatam; piso 23%) |

* Oráculo: 3 577 311 / 3 577 311 respostas iguais à referência em cada versão.
* 3.11.11 dá os mesmos vereditos (R2).
* **Achado negativo:** V3H05, admitido pela diversidade estrutural, é 1,2–2,9× mais lento que os baselines.
* O NNLS da V9 aparece só como `RETROSPECTIVE_BASELINE_ONLY` (`v3_unified/analysis/metrics.json`).

## Reproduzir

```bash
./reproduce.sh setup && ./reproduce.sh analysis    # refaz V2/V3 se os traces brutos estiverem presentes (ver abaixo)
./reproduce.sh v2 rubis                             # nós 4.1.12 + 3.11.11, import, oráculo, pré-registro, medição (~4 h)
./reproduce.sh v3                                   # campanha V3 (nós novos sl_v3_rubis*)
```

Os traces brutos (`*/outputs/**/raw/*.jsonl.gz`, ~2,2 GB por campanha) não estão no git. O SHA-256 da V3 está em
`v3_unified/benchmark/run1/RAW_SHA256.json`. Resumos, contrastes, a coorte congelada (`frozen/cohort_source.pkl`) e a
referência do oráculo (`frozen/ORACLE_REF.pkl`) estão versionados. Por isso a medição pode ser refeita do zero e o
oráculo confere cada resposta.

A V9 original (`v9_original/`) é registro histórico: sua fase de projeto leu o `rubis_full` em escala completa,
que só existe na máquina onde a V9 foi executada. As saídas E1–E23 estão em `v9_original/runs/v9_generation_001/`.
