#!/usr/bin/env bash
set -euo pipefail
python3 -m methodology.tpcx_iot.stage91.cli dry-run
python3 -m unittest discover -s tests -v
