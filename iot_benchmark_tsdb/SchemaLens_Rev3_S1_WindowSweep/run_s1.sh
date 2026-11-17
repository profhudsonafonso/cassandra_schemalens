#!/usr/bin/env bash
# S1: uses existing B tables via SELECT only; never creates/drops data.
set -euo pipefail
if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo 'Usage: bash run_s1.sh prepare|verify|run|pack OUTPUT_DIRECTORY [HOST]' >&2
  exit 2
fi
ACTION="$1";OUT="$(realpath -m "$2")";HOST="${3:-127.0.0.1}"
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(realpath "$BASE/..")"
REV2="$ROOT/SchemaLens_V4_Rev2_Confirmatory"
REV3="$ROOT/SchemaLens_V4_Rev3_PlanAware"
B_WORKSPACE="${S1_B_WORKSPACE:-$(realpath -m "$ROOT/../schemalens_rev3_new_java_B_20261105_193618")}" 
if [[ ! -f "$REV2/v4_rev2/runtime_helpers.py" || ! -f "$REV3/rev3/runtime.py" ]];then
  echo 'Install original Rev2/Rev3 packages adjacent to S1 first' >&2;exit 3
fi
if [[ ! -f "$B_WORKSPACE/HOLDOUT_GENERATION_OUTPUT.json" ]];then
  echo "Missing original campaign B workspace: $B_WORKSPACE" >&2;exit 4
fi
EXP="$(python3 - "$B_WORKSPACE" <<'PY'
import json,sys
from pathlib import Path
print(json.loads((Path(sys.argv[1])/'HOLDOUT_GENERATION_OUTPUT.json').read_text())['isolated_v3_experiment'])
PY
)"
export PYTHONPATH="$BASE:$ROOT/SchemaLens_Rev3_Independent_Confirmatory:$REV3:$REV2:$EXP${PYTHONPATH:+:$PYTHONPATH}"
cd "$EXP"
CMD=(python3 "$BASE/s1_sweep.py" "$ACTION" --workspace "$B_WORKSPACE" --out "$OUT")
if [[ "$ACTION" == run ]]; then
  CMD+=(--host "$HOST" --port "${S1_PORT:-9043}")
elif [[ "$ACTION" == pack ]]; then
  CMD+=(--archive "$ROOT/rev3_S1_window_sweep_results.zip")
fi
"${CMD[@]}"
