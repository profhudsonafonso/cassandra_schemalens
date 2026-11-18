#!/usr/bin/env bash
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(realpath "$BASE/..")"
export PYTHONPATH="$BASE:$PROJECT/SchemaLens_V4_Rev3_PlanAware:$PROJECT/SchemaLens_V4_Rev2_Confirmatory"
python3 -m unittest discover -s "$BASE/tests" -p 'test_*.py' -v
