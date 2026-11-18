#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m methodology.tpcx_iot.stage8.cli
python3 -m examples.show_stage8_hbench
python3 -m unittest discover -s tests -v
