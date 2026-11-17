#!/usr/bin/env bash
# A READ-ONLY probe of existing Rev2 materializations. Never creates keyspaces.
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -lt 1 ]];then echo 'Usage: run_probe.sh FROZEN_REV2_EXPERIMENT --results PATH --out NEW_DIR [--host ... --port ...]' >&2;exit 2;fi
EXP="$(realpath "$1")";shift
REV2="$(realpath "$BASE/../SchemaLens_V4_Rev2_Confirmatory")"
if [[ ! -f "$REV2/v4_rev2/prebenchmark.py" ]];then echo "Missing neighboring Rev2 add-on: $REV2" >&2;exit 3;fi
cd "$EXP";export PYTHONPATH="$EXP:$BASE:$REV2"
python3 -m rev3.probe --experiment "$EXP" "$@"
