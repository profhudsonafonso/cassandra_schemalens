#!/usr/bin/env bash
# RUBiS V9 -> protocolo comum v1 NESTE SERVIDOR: dados regenerados, Cassandra 4.1.12 e 3.11.11 (mesmos dados).
# Uso: ./run_server.sh gen|node|import|oracle|smoke|freeze|measure|status|analyze|accept|report
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$HERE/../.venv/bin/python"
RUN="${RUN:-outputs/v1_run1}"
cd "$HERE"; mkdir -p outputs
case "${1:-}" in
  gen)     "$PY" rubis_v1/gen_data.py ;;
  node)    "$PY" rubis_v1/measure.py node --arm v41 && "$PY" rubis_v1/measure.py node --arm v311 ;;
  import)  "$PY" rubis_v1/transfer.py import --arm v41 && "$PY" rubis_v1/transfer.py import --arm v311 ;;
  oracle)  "$PY" rubis_v1/oracle_ref.py --arm v41 ;;
  smoke)   "$PY" rubis_v1/measure.py run --smoke --out "outputs/smoke_$(date +%s)" ;;
  freeze)  "$PY" rubis_v1/freeze.py ;;
  measure) setsid nohup bash -c "'$PY' rubis_v1/measure.py run --out '$RUN' > 'outputs/measure_$(basename "$RUN").log' 2>&1; echo EXIT \$? >> 'outputs/measure_$(basename "$RUN").log'" >/dev/null 2>&1 < /dev/null &
           echo "medicao desacoplada (sobrevive ao fim da sessao). Acompanhe: ./run_server.sh status" ;;
  status)  cat "$RUN/status.json" 2>/dev/null || true; tail -2 "outputs/measure_$(basename "$RUN").log" 2>/dev/null || true ;;
  analyze) "$PY" rubis_v1/analyze.py --run "$RUN" ;;
  accept)  "$PY" rubis_v1/acceptance.py --run "$RUN" ;;
  report)  "$PY" rubis_v1/make_tables.py --run "$RUN" && (cd relatorio_overleaf && pdflatex -interaction=nonstopmode main.tex >/dev/null; pdflatex -interaction=nonstopmode main.tex | grep "Output written") ;;
  *) echo "uso: $0 gen|node|import|oracle|smoke|freeze|measure|status|analyze|accept|report"; exit 2 ;;
esac
