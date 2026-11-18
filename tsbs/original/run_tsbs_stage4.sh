#!/usr/bin/env bash
# New file only. Run from any directory after adding to SchemaLens_TSBS_TSDB_Only root.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
python3 -m methodology.tsbs.stage4.cli "$@"
