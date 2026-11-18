#!/usr/bin/env bash
# Ensure the frozen, isolated V3 is imported, never the outer project.
set -euo pipefail
if [[ $# -lt 4 ]]; then
  echo 'Usage: bash SchemaLens_V4_Canonical_CrossReuse/run_v4.sh --experiment PATH --mode offline|live [options]' >&2
  exit 2
fi
ADDON="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
argv=("$@")
EXP=''
EXP_IDX=-1
EXP_EQUALS=0
for ((i=0;i<${#argv[@]};i++)); do
  if [[ "${argv[$i]}" == '--experiment' ]]; then
    if ((i+1 >= ${#argv[@]})); then echo 'Missing --experiment path' >&2; exit 2; fi
    EXP="${argv[$((i+1))]}"
    EXP_IDX=$((i+1))
    EXP_EQUALS=0
  elif [[ "${argv[$i]}" == --experiment=* ]]; then
    EXP="${argv[$i]#--experiment=}"
    EXP_IDX=$i
    EXP_EQUALS=1
  fi
done
if [[ -z "$EXP" ]]; then echo 'Explicit --experiment required' >&2; exit 2; fi
if [[ ! -d "$EXP" ]]; then echo "Experiment path does not exist: $EXP" >&2; exit 2; fi
EXP_ABS="$(cd "$EXP" && pwd)"
if [[ ! -f "$EXP_ABS/outputs/stage8/28_hbench_frozen.json" || ! -f "$EXP_ABS/iot_pipeline/native_reference_v3.py" ]]; then
  echo "Invalid isolated frozen V3 experiment or missing baseline V3: $EXP_ABS" >&2
  exit 2
fi
# Normalise the argument *before* changing directories, including --experiment=.
if [[ "$EXP_EQUALS" -eq 1 ]]; then
  argv[$EXP_IDX]="--experiment=$EXP_ABS"
else
  argv[$EXP_IDX]="$EXP_ABS"
fi
cd "$EXP_ABS"
# The current directory is prepended to Python sys.path by `python -m`.
# This is why PYTHONPATH alone was insufficient in the previous release.
export PYTHONPATH="$ADDON:$EXP_ABS"
exec python3 -m diagnostic_v4.cli "${argv[@]}"
