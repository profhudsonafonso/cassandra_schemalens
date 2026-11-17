#!/usr/bin/env bash
# Execute a new post-hoc Q8 diagnostic in the *isolated frozen experiment* import context.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if (( $# < 1 )); then
  echo "Usage: bash $0 FROZEN_EXP --addon ABS_PATH --results ABS_PATH --out NEW_PATH [--pairs 200 ...]" >&2
  exit 2
fi
EXP="$(realpath "$1")"; shift
ADDON=""
ARGS=("$@")
for (( i=0; i<${#ARGS[@]}; i++ )); do
  if [[ "${ARGS[$i]}" == "--addon" ]]; then
    (( i+1 < ${#ARGS[@]} )) || { echo "Missing --addon argument" >&2; exit 2; }
    ADDON="$(realpath "${ARGS[$((i+1))]}")"
  fi
done
[[ -n "$ADDON" && -f "$ADDON/v4_rev2/prebenchmark.py" ]] || { echo "Invalid --addon directory" >&2; exit 2; }
[[ -f "$EXP/iot_pipeline/native_reference_v3.py" ]] || { echo "Missing isolated native_reference_v3.py" >&2; exit 2; }
cd "$EXP"
# Drop old env PYTHONPATH to prevent accidental import from the project root.
export PYTHONPATH="$EXP:$ADDON:$HERE:$HERE/../SchemaLens_Rev2_TS0_Telemetry"
python3 "$HERE/e5_q8.py" --experiment "$EXP" "${ARGS[@]}"
