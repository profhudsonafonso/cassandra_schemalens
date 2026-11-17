#!/usr/bin/env bash
# Run A1 and stages 4-8 over a native IoT Benchmark generated CSV dataset plus
# a native GenerateQueryWorkLoad parameter-object capture. NO benchmark.
set -euo pipefail
if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "Usage: $0 NATIVE_CSV_DIR JAVA_CAPTURE.jsonl GENERATOR_config.properties [MAX_CELLS]" >&2
  exit 2
fi
cd "$(dirname "$0")/.."
NATIVE="$(realpath "$1")"
CAPTURE="$(realpath "$2")"
CONFIG="$(realpath "$3")"
MAX_CELLS="${4:-500000}"
if [[ ! -f "$NATIVE/schema.txt" || ! -f "$CAPTURE" || ! -f "$CONFIG" ]]; then
  echo 'Native schema.txt, JSONL or config.properties missing' >&2
  exit 3
fi
if [[ "$MAX_CELLS" =~ ^[0-9]+$ ]] && [[ "$MAX_CELLS" -gt 0 ]]; then
  :
else
  echo 'MAX_CELLS must be positive integer' >&2
  exit 3
fi
python3 phase_a1/tools/run_a1.py --native-data "$NATIVE" \
  --official-query-objects "$CAPTURE" --properties "$CONFIG" --max-rows "$MAX_CELLS"
python3 -m iot_pipeline.run
python3 -m iot_pipeline.stage9 --dry-run
# Exclude the deliberately frozen reference-baseline fixture tests: those tests
# assert Pilot A1's exact query parameters and are not general native-input tests.
python3 -m unittest discover -s tests -p 'test_iot_stages_4_to_9.py' -v
python3 -m unittest discover -s tests -p 'test_stage4_rev2.py' -v
python3 -m unittest discover -s tests -p 'test_official_bridge_v3.py' -v
printf '\nNative Java-object sample adapted, Stage8 frozen, Stage9 NOT measured.\n'
