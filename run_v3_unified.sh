#!/usr/bin/env bash
# SchemaLens V3 - Unified Stage 7 / Stage 8 for IoT-Benchmark, TPCx-IoT, TSBS cpu-only and RUBiS.
# Modes: --audit | --test | --prebenchmark | --freeze | --smoke | --benchmark | --analyze | --all | --status
# Long Cassandra runs are detached (setsid nohup) and survive the end of the SSH session.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
PY="$HERE/versao_ajustada/.venv/bin/python"
BENCHES=(iot tpcx_iot tsbs rubis)
ANALYSIS_CPUS="${ANALYSIS_CPUS:-16-23}"

detach() {  # detach <log> <command...>
  local log="$1"; shift
  mkdir -p "$(dirname "$log")"
  setsid nohup bash -c "$*; echo EXIT \$? >> '$log'" >"$log" 2>&1 < /dev/null &
  echo "detached -> $log"
}

case "${1:-}" in
  --audit)
    git rev-parse HEAD; git status --short | head -20
    docker --version; "$PY" --version
    "$PY" -c "import json;print(json.load(open('versao_ajustada/common/protocol_v1.json'))['environment']['cassandra_image'])"
    echo "audit report: docs/V3_STAGE7_AUDIT.md" ;;
  --test)
    taskset -c "$ANALYSIS_CPUS" "$PY" -m pytest unified_stage7_v3/tests -q ;;
  --prebenchmark)
    for b in "${BENCHES[@]}"; do taskset -c "$ANALYSIS_CPUS" "$PY" -m unified_stage7_v3.run_prebenchmark "$b"; done
    "$PY" -m unified_stage7_v3.report_prebenchmark ;;
  --freeze)
    taskset -c "$ANALYSIS_CPUS" "$PY" -m pytest unified_stage7_v3/tests -q
    for b in "${BENCHES[@]}"; do "$PY" -m unified_stage7_v3.freeze freeze "$b"; done ;;
  --smoke)
    "$PY" -m unified_stage7_v3.stage9.tsbs_v3 run --smoke
    "$PY" -m unified_stage7_v3.stage9.rubis_v3 node --smoke
    "$PY" -m unified_stage7_v3.stage9.rubis_v3 import --smoke
    "$PY" -m unified_stage7_v3.stage9.rubis_v3 run --smoke ;;
  --benchmark)
    "$PY" -m unified_stage7_v3.stage9.reuse_v2 iot
    "$PY" -m unified_stage7_v3.stage9.reuse_v2 tpcx_iot
    detach outputs_v3/benchmark_tsbs_rubis.log \
      "'$PY' -m unified_stage7_v3.stage9.tsbs_v3 run && '$PY' -m unified_stage7_v3.stage9.rubis_v3 node && '$PY' -m unified_stage7_v3.stage9.rubis_v3 import && '$PY' -m unified_stage7_v3.stage9.rubis_v3 run" ;;
  --analyze)
    taskset -c "$ANALYSIS_CPUS" "$PY" -m unified_stage7_v3.analysis.run_all ;;
  --status)
    for b in tsbs rubis; do f="outputs_v3/$b/benchmark/run1/status.json"; [ -f "$f" ] && { echo "$b:"; cat "$f"; echo; }; done
    tail -3 outputs_v3/benchmark_tsbs_rubis.log 2>/dev/null || true ;;
  --all)
    "$0" --audit && "$0" --prebenchmark && "$0" --freeze && "$0" --benchmark
    echo "benchmark detached; run '$0 --analyze' when '$0 --status' shows EXIT 0" ;;
  *) echo "usage: $0 --audit|--test|--prebenchmark|--freeze|--smoke|--benchmark|--analyze|--status|--all"; exit 2 ;;
esac
