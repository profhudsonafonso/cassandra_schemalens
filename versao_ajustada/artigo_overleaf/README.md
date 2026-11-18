# Artigo — SchemaLens no Apache Cassandra (quatro benchmarks, protocolo comum v1)

`main.tex` (pdfLaTeX, pronto para o Overleaf: suba a pasta inteira, com `tables/` e `figs/`). As tabelas são geradas a
partir das análises v1 dos quatro experimentos; nenhum número das tabelas é digitado à mão:

```bash
../.venv/bin/python build_tables.py     # regenera tables/*.tex e copia figs/*.pdf
pdflatex main.tex && pdflatex main.tex
```

Estrutura: metodologia (fases de projeto e de medição; protocolo v1; análise de consistência), volume de dados de cada
experimento, experimentos TSDB (IoT-Benchmark, TPCx-IoT, TSBS), RUBiS em seção separada (4.1.12 × 3.11.11), aceitação,
discussão, ameaças à validade.
