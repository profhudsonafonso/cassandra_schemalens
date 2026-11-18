#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
python3 -m iot_pipeline.run "$@"
python3 -m iot_pipeline.stage9 --dry-run
python3 -m unittest discover -s tests -v
printf '\nStage8 is frozen. Stage9 local Cassandra was NOT executed.\n'
