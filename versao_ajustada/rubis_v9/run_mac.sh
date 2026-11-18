#!/usr/bin/env bash
# RUBiS V9 -> protocolo comum v1 (Cassandra 4.1.12). Rodar NO MAC onde a V9 foi executada.
# Uso: ./run_mac.sh <passo>   (setup | check | node | export | import | smoke | freeze | measure | analyze | accept | report | status)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VA="$HERE/.."
PY="$VA/.venv/bin/python"
RUN="${RUN:-outputs/v1_run1}"
cd "$HERE"
case "${1:-}" in
  setup)
    [[ -x "$PY" ]] || python3 -m venv "$VA/.venv"
    "$PY" -m pip install -q cassandra-driver numpy matplotlib pyyaml
    "$PY" -c "import cassandra, numpy, yaml; print('ok driver', cassandra.__version__)" ;;
  check)
    echo "== container fonte (3.11.11):"; docker ps --format '{{.Names}} {{.Image}} {{.Status}} {{.Ports}}' | grep -i "${RUBIS_SOURCE_CONTAINER:-rubis-schemalens-cassandra}" || { echo "FALTA o container fonte"; exit 1; }
    echo "== repositorio V8:"; ls -d ~/Documents/rubis3_claude/experiments/rubis_cassandra/outputs/final_run_v8_completion_011 || { echo "FALTA o repositorio V8 (referencia de corretude)"; exit 1; }
    echo "== latencias brutas da V9 (opcional, para comparar 3.11 x 4.1):"; ls ../../runs/v9_generation_001/stages/E20_benchmark/raw 2>/dev/null | head -3 || echo "(ausentes: a comparacao 3.11 x 4.1 sera omitida)"
    echo "== escritores concorrentes (devem estar parados):"; pgrep -fl "rubis_cassandra/.venv/bin/python|schemalens_v9.load" || echo "nenhum"
    echo "== Docker:"; docker info --format 'CPUs={{.NCPU}} Mem={{.MemTotal}} Arch={{.Architecture}}' ;;
  node)    "$PY" rubis_v1/measure.py node ;;
  export)  "$PY" rubis_v1/transfer.py export ;;
  import)  "$PY" rubis_v1/transfer.py import ;;
  smoke)   "$PY" rubis_v1/measure.py run --smoke --out "outputs/smoke_$(date +%s)" ;;
  freeze)  "$PY" rubis_v1/freeze.py ;;
  measure) nohup "$PY" rubis_v1/measure.py run --out "$RUN" > "outputs/measure_$(basename "$RUN").log" 2>&1 &
           echo "medicao em segundo plano (PID $!). Acompanhe: ./run_mac.sh status" ;;
  status)  cat "$RUN/status.json" 2>/dev/null || echo "sem status ainda"; tail -3 "outputs/measure_$(basename "$RUN").log" 2>/dev/null || true ;;
  analyze) "$PY" rubis_v1/analyze.py --run "$RUN" ;;
  accept)  "$PY" rubis_v1/acceptance.py --run "$RUN" ;;
  report)  "$PY" rubis_v1/make_tables.py --run "$RUN" && (cd relatorio_overleaf && (command -v pdflatex >/dev/null && pdflatex -interaction=nonstopmode main.tex >/dev/null && pdflatex -interaction=nonstopmode main.tex >/dev/null && echo "relatorio_overleaf/main.pdf" || echo "pdflatex ausente: envie relatorio_overleaf/ ao Overleaf")) ;;
  *) echo "uso: $0 setup|check|node|export|import|smoke|freeze|measure|status|analyze|accept|report"; exit 2 ;;
esac
