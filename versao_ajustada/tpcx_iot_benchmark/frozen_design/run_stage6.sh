#!/usr/bin/env bash
set -euo pipefail
python3 -m methodology.tpcx_iot.stage6.cli
python3 -m unittest discover -s tests -v
