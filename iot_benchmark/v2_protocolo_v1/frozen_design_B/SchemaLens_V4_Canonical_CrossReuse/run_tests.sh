#!/usr/bin/env bash
# Force isolated V3 as the Python import root; preserve frozen experimental files.
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo 'Usage: bash SchemaLens_V4_Canonical_CrossReuse/run_tests.sh path/to/isolated_V3_experiment' >&2
  exit 2
fi
ADDON="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXP="$(cd "$1" && pwd)"
if [[ ! -f "$EXP/iot_pipeline/native_reference_v3.py" || ! -f "$EXP/outputs/stage8/28_hbench_frozen.json" ]]; then
  echo "Invalid isolated V3 experiment or native-reference add-on is missing: $EXP" >&2
  exit 2
fi
# -m normally inserts the current working directory at the front of sys.path.
# Running from the outer project would shadow EXP/iot_pipeline even if PYTHONPATH
# included EXP. Enter EXP before invoking Python and discard inherited paths.
cd "$EXP"
export PYTHONPATH="$ADDON:$EXP"
python3 -m unittest discover -s "$ADDON/tests" -p 'test_v4.py' -v
