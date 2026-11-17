#!/usr/bin/env bash
# Run V4 Rev2 stage7/stage8 without Cassandra. Force fresh/isolated iot_pipeline imports.
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -lt 1 ]]; then echo "Usage: $0 ABSOLUTE_FROZEN_EXPERIMENT [--confirmatory --reference-v3 OLD_EXPERIMENT] [--verify-only]" >&2; exit 2; fi
EXP="$(realpath "$1")"; shift
ARGS=("$@")
cd "$EXP"
export PYTHONPATH="$EXP:$BASE${PYTHONPATH:+:$PYTHONPATH}"
python3 -m v4_rev2.prebenchmark --experiment "$EXP" "${ARGS[@]}"
