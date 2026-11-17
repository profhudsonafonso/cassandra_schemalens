#!/usr/bin/env bash
# One-command OFFLINE installation after unzip into SchemaLens_IoT_Cassandra_V3_Automatico.
set -euo pipefail
ADDON="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$ADDON/scripts/install_native_reference_v3.py" \
  --auto \
  --expected-hbench 2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb
