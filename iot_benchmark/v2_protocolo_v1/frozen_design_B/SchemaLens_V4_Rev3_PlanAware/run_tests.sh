#!/usr/bin/env bash
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REV2="$(realpath "$BASE/../SchemaLens_V4_Rev2_Confirmatory")"
[[ -f "$REV2/v4_rev2/cost.py" ]] || { echo 'Rev2 add-on is missing alongside this package' >&2; exit 3; }
export PYTHONPATH="$BASE:$REV2"
python3 -m unittest discover -s "$BASE/tests" -p 'test_*.py' -v
