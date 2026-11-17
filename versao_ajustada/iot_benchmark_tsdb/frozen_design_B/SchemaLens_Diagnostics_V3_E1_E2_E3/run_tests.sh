#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo 'Usage: bash SchemaLens_Diagnostics_V3_E1_E2_E3/run_tests.sh <isolated-frozen-experiment>' >&2
  exit 2
fi
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export E123_TEST_BASE="$(realpath "$1")"
export PYTHONPATH="$HERE:$E123_TEST_BASE${PYTHONPATH:+:$PYTHONPATH}"
python3 -m unittest discover -s "$HERE/tests" -p 'test_diagnostic_e123.py' -v
