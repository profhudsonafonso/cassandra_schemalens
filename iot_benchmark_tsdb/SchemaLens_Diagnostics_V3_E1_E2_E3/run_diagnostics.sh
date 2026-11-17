#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "$#" -lt 1 ]]; then
  echo 'Usage: bash SchemaLens_Diagnostics_V3_E1_E2_E3/run_diagnostics.sh --experiment ... --mode preflight|e1|e2|e3 [opts]' >&2
  exit 2
fi
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
python3 -m diagnostic_e123.cli "$@"
