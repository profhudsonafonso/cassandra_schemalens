#!/usr/bin/env bash
# =============================================================================================================
#  SchemaLens / Cassandra — reprodução do repositório.
#
#  Offline (sem Cassandra, minutos):
#    ./reproduce.sh setup        cria ./.venv com as versões fixadas (requirements-lock.txt)
#    ./reproduce.sh env          confere Python, Docker e os digests das imagens Cassandra do protocolo
#    ./reproduce.sh test         15 testes obrigatórios da V3 (41 casos)
#    ./reproduce.sh verify       atestação de relocação + verificação dos 4 freezes V3 (reexecuta o Stage 7/8)
#    ./reproduce.sh analysis     reexecuta as análises V2 (4 benchmarks) e V3 a partir das latências brutas
#                                versionadas e compara com os resultados versionados
#    ./reproduce.sh articles     regenera tabelas/figuras e compila os artigos V2 e V3 (pdflatex)
#    ./reproduce.sh offline      setup + env + test + verify + analysis
#
#  Com Cassandra (horas; Docker com >= 24 CPUs lógicas e 16 GB livres; ver README.md):
#    ./reproduce.sh v2 <iot|tpcx_iot|tsbs|rubis>   campanha V2 completa do benchmark (protocolo comum v1)
#    ./reproduce.sh v3                             campanha V3 (reuso IoT/TPCx; TSBS e RUBiS em segundo plano)
# =============================================================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
PY="$ROOT/.venv/bin/python"
CPUS="${ANALYSIS_CPUS:-}"
run_py() { if [[ -n "$CPUS" ]]; then taskset -c "$CPUS" "$PY" "$@"; else "$PY" "$@"; fi; }
need_env() { [[ -x "$PY" ]] || { echo "ambiente ausente: rode ./reproduce.sh setup"; exit 2; }; }

case "${1:-}" in
  setup)
    ./setup_env.sh ;;
  env)
    need_env
    "$PY" --version
    docker --version || echo "Docker ausente: só os modos offline funcionam"
    "$PY" -c "import json; e=json.load(open('methodology/protocolo_v1/protocol_v1.json'))['environment']; print('Cassandra (protocolo v1):', e['cassandra_release_version'], e['cassandra_image'])"
    "$PY" -c "import json; a=json.load(open('rubis/v2_protocolo_v1/PREREGISTRATION_RUBIS_V1.json')); print('Cassandra 3.11.11 (RUBiS secundário):', a['arms']['v311']['image'])" ;;
  test)
    need_env; (cd methodology && run_py -m pytest unified_stage7_v3/tests -q) ;;
  verify)
    need_env
    (cd methodology && run_py -m unified_stage7_v3.relocation check)
    for b in iot tpcx_iot tsbs rubis; do (cd methodology && run_py -m unified_stage7_v3.freeze verify "$b"); done ;;
  analysis)
    need_env; run_py methodology/reproduce_check.py v3 iot tpcx_iot tsbs rubis ;;
  articles)
    need_env
    run_py methodology/artigos/v2_protocolo_v1/build_tables.py
    run_py methodology/artigos/v3_unified/build_tables.py
    for d in methodology/artigos/v2_protocolo_v1 methodology/artigos/v3_unified; do
      (cd "$d" && pdflatex -interaction=nonstopmode main.tex >/dev/null && pdflatex -interaction=nonstopmode main.tex | grep "Output written" || true)
    done ;;
  offline)
    "$0" setup && "$0" env && "$0" test && "$0" verify && "$0" analysis ;;
  v2)
    need_env
    case "${2:-}" in
      iot)      (cd iot_benchmark/v2_protocolo_v1 && ./run_all.sh all) ;;
      tpcx_iot) (cd tpcx_iot/v2_protocolo_v1 && ./run_all.sh all) ;;
      tsbs)     (cd tsbs/v2_protocolo_v1 && ./gen_data.sh && ./run_all.sh all) ;;
      rubis)    (cd rubis/v2_protocolo_v1 && for s in node import oracle freeze measure; do ./run_server.sh "$s"; done) ;;
      *) echo "uso: $0 v2 <iot|tpcx_iot|tsbs|rubis>"; exit 2 ;;
    esac ;;
  v3)
    need_env; methodology/run_v3_unified.sh --benchmark ;;
  *)
    sed -n 2,19p "$0"; exit 2 ;;
esac
