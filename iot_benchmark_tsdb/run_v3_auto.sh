#!/usr/bin/env bash
# Single-command launcher: no Cassandra server, Docker or benchmark execution.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$ROOT/scripts/auto_official_v3.py" "$@"
