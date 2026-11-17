#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m methodology.tpcx_iot.stage9.cli dry-run
python3 -m unittest discover -s tests -v
printf '\nDry-run and tests completed. For LIVE Cassandra follow docs/ETAPA9_CASSANDRA_GUIA_PT.md\n'
