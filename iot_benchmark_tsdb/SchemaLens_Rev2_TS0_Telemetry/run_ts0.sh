#!/usr/bin/env bash
# Run only existing frozen TS0 SELECT routes in the isolated Rev2 Python context.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -lt 1 ]]; then
  echo "Usage: bash $0 FROZEN_EXPERIMENT --results PATH --addon PATH --out NEW_PATH [--rounds 3 --port 9043]" >&2
  exit 2
fi
EXP="$(realpath "$1")"; shift
# The Rev2 implementation was frozen separately; import it explicitly, not
# the unrelated iot_pipeline tree in the project working directory.
ADDON=""
ARGS=("$@")
for ((i=0; i<${#ARGS[@]}; i++)); do
  if [[ "${ARGS[$i]}" == "--addon" ]]; then
    if (( i+1 >= ${#ARGS[@]} )); then echo "Missing --addon value" >&2; exit 2; fi
    ADDON="$(realpath "${ARGS[$((i+1))]}")"
  fi
done
if [[ -z "$ADDON" || ! -f "$ADDON/v4_rev2/prebenchmark.py" ]]; then
  echo "Missing or invalid --addon PATH/SchemaLens_V4_Rev2_Confirmatory" >&2
  exit 2
fi
cd "$EXP"
export PYTHONPATH="$EXP:$ADDON:$HERE${PYTHONPATH:+:$PYTHONPATH}"
python3 "$HERE/ts0_telemetry.py" --experiment "$EXP" "${ARGS[@]}"
