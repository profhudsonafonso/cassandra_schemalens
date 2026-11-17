#!/usr/bin/env bash
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHONPATH="$BASE${PYTHONPATH:+:$PYTHONPATH}" python3 -m unittest discover -s "$BASE/tests" -v
