#!/usr/bin/env bash
set -euo pipefail
# Stage 9 add-on does NOT install Cassandra or delete any keyspace by default.
# Pass arguments explicitly; default is intentionally offline.
python3 -m methodology.tsbs.stage9.cli "$@"
