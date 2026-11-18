#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
python3 -m unittest discover -s tests -v
python3 tools/run_a1.py "$@"
