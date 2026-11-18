# Artigo V3 (Overleaf)

`main.tex` — SchemaLens V3: Stage 7/8 unificado avaliado em IoT-Benchmark, TPCx-IoT, TSBS cpu-only e RUBiS.

* Compilar no Overleaf com **pdfLaTeX** (enviar `main.tex`, `tables/` e `figs/`).
* Todas as tabelas (`tables/*.tex`) e figuras (`figs/*.pdf`) são geradas a partir de `outputs_v3/`:

```
.venv/bin/python methodology/artigos/v3_unified/build_tables.py
cd methodology/artigos/v3_unified && pdflatex main.tex && pdflatex main.tex
```

Fontes: `<benchmark>/v3_unified/{prebenchmark,analysis}` e `methodology/docs/v3/` (inclui o decision log).
