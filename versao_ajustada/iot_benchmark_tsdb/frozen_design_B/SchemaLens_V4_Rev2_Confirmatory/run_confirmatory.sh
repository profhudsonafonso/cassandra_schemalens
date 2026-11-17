#!/usr/bin/env bash
# Only frozen V4 Rev2 holdout HBench can be benchmarked in disposable Cassandra keyspaces.
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -lt 1 ]]; then echo "Usage: $0 FRESH_EXPERIMENT [--rounds 40 --host 127.0.0.1 --port 9043 --confirm-reset RESET_ONLY_SCHEMA_CONF_R2_KEYSPACES]" >&2; exit 2; fi
EXP="$(realpath "$1")"; shift
cd "$EXP"
export PYTHONPATH="$EXP:$BASE${PYTHONPATH:+:$PYTHONPATH}"
python3 -m v4_rev2.benchmark --experiment "$EXP" "$@"
