#!/usr/bin/env bash
# Execute outside the V3 working tree; force the isolated V3 package to win imports.
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -lt 1 ]]; then
  echo 'Usage: bash SchemaLens_V4_Auto_Prebenchmark/run_v4_auto.sh PATH_TO_FROZEN_V3 [--verify-only]' >&2
  exit 2
fi
EXPERIMENT="$(realpath "$1")"
shift
if [[ ! -f "$EXPERIMENT/outputs/stage8/28_hbench_frozen.json" ]]; then
  echo 'Missing frozen V3 experiment' >&2; exit 2
fi
cd "$EXPERIMENT"
# IMPORTANT: use PWD as first package source, even with external PYTHONPATH.
export PYTHONPATH="$EXPERIMENT:$BASE${PYTHONPATH:+:$PYTHONPATH}"
python3 -m v4_auto.cli --experiment "$EXPERIMENT" "$@"
