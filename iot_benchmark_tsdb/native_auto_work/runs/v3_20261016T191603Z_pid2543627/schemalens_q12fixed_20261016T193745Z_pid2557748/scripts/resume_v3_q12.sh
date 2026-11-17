#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo 'Usage: bash scripts/resume_v3_q12.sh RUN_DIR [MAX_CELLS]' >&2
  exit 2
fi
python3 scripts/resume_v3_q12.py --run-dir "$1" --max-cells "${2:-500000}"
