#!/usr/bin/env bash
# SchemaLens TSBS - versao ajustada (protocolo comum v1).
# Passos: 0 smoke (opcional) -> 1 pre-registro -> 2 medicao -> 3 analise -> 4 ablacoes -> 5 aceitacao -> 6 relatorio
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${PY:-$HERE/../../.venv/bin/python}"   # venv único na raiz do repositório (./setup_env.sh)
RUN="${RUN:-outputs/v1_run1}"
cd "$HERE"
case "${1:-all}" in
  smoke)    "$PY" tsbs_v1/measure.py --smoke --out "outputs/smoke_$(date +%s)" ;;
  freeze)   "$PY" tsbs_v1/freeze.py ;;
  measure)  "$PY" tsbs_v1/measure.py --out "$RUN" ;;
  analyze)  "$PY" tsbs_v1/analyze.py --run "$RUN" ;;
  ablation) "$PY" tsbs_v1/ablation.py --run "$RUN" ;;
  accept)   "$PY" tsbs_v1/acceptance.py --run "$RUN" ;;
  report)   (cd relatorio_overleaf && pdflatex -interaction=nonstopmode main.tex >/dev/null && pdflatex -interaction=nonstopmode main.tex >/dev/null && echo "relatorio_overleaf/main.pdf") ;;
  all)
    [[ -f PREREGISTRATION_TSBS_V1.json ]] || "$PY" tsbs_v1/freeze.py
    "$PY" tsbs_v1/measure.py --out "$RUN"
    "$PY" tsbs_v1/analyze.py --run "$RUN"
    "$PY" tsbs_v1/ablation.py --run "$RUN"
    "$PY" tsbs_v1/acceptance.py --run "$RUN"
    ;;
  *) echo "uso: $0 [smoke|freeze|measure|analyze|ablation|accept|report|all]"; exit 2 ;;
esac
