#!/usr/bin/env bash
# SchemaLens Rev3 independent Cassandra confirmation: two predeclared Java campaigns.
# This NEVER drops or overwrites Cassandra keyspaces or frozen experiment evidence.
set -euo pipefail
if [[ $# -ne 2 ]]; then
  echo "Usage: bash run_campaign.sh A|B generate|freeze|preflight|benchmark|show-flush|storage|pack" >&2
  exit 2
fi
CAMPAIGN="$1";ACTION="$2"
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(realpath "$BASE/..")"
REV2="$PROJECT/SchemaLens_V4_Rev2_Confirmatory"
REV3="$PROJECT/SchemaLens_V4_Rev3_PlanAware"
# The original V3 and previous confirmed Rev2 experiments are input-only.
REFERENCE_V3="${REFERENCE_V3:-$PROJECT/native_auto_work/runs/v3_20261016T191603Z_pid2543627/schemalens_q12fixed_20261016T193745Z_pid2557748}"
REFERENCE_REV2="${REFERENCE_REV2:-$(realpath -m "$PROJECT/../schemalens_confirmatorio_rev2/java_work/runs/v3_20261026T200853Z_pid4169024/schemalens_rev2_ts5_contract_validated")}"
if [[ ! -f "$REV2/v4_rev2/prepare_holdout.py" || ! -f "$REV3/rev3/runtime.py" ]]; then
  echo "Missing original audited Rev2 or Rev3 add-on alongside this package" >&2;exit 3
fi
case "$CAMPAIGN" in
  A)
    DEVICES=6;LOOPS=300;BATCH=5;MAX_CELLS=100000
    DATA_SEED=531007;QUERY_SEED=531013;QUERY_INTERVAL_MS=86400000
    MEAS_SEED=130713;TAG='rev3A_20261105'
    ;;
  B)
    DEVICES=9;LOOPS=600;BATCH=5;MAX_CELLS=100000
    DATA_SEED=531101;QUERY_SEED=531107;QUERY_INTERVAL_MS=604800000
    MEAS_SEED=131117;TAG='rev3B_20261105'
    ;;
  *) echo "Choose A or B" >&2;exit 2;;
esac
WORK="${WORKSPACE_OVERRIDE:-$(realpath -m "$PROJECT/../schemalens_rev3_new_java_${CAMPAIGN}_20261105")}" 
export PYTHONPATH="$BASE:$REV3:$REV2"
case "$ACTION" in
 generate)
    cd "$PROJECT"
    python3 -m rev3_confirmatory.campaign generate \
      --source-project "$PROJECT" --reference-v3 "$REFERENCE_V3" \
      --reference-rev2 "$REFERENCE_REV2" --workspace "$WORK" \
      --devices "$DEVICES" --loops "$LOOPS" --batch-size "$BATCH" \
      --max-cells "$MAX_CELLS" --point-step-ms 600000 \
      --query-interval-ms "$QUERY_INTERVAL_MS" --group-by-ms 3600000 \
      --data-seed "$DATA_SEED" --query-seed "$QUERY_SEED"
    ;;
 freeze|preflight|benchmark|show-flush|storage|pack)
    if [[ ! -f "$WORK/HOLDOUT_GENERATION_OUTPUT.json" ]];then
      echo "No completed new Java generation: $WORK" >&2;exit 4
    fi
    NEW_EXP="$(python3 - "$WORK" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]).resolve()/'HOLDOUT_GENERATION_OUTPUT.json'
print(json.loads(p.read_text())['isolated_v3_experiment'])
PY
)"
    [[ -d "$NEW_EXP" ]] || { echo "Experiment missing: $NEW_EXP" >&2;exit 4; }
    cd "$NEW_EXP"
    export PYTHONPATH="$NEW_EXP:$BASE:$REV3:$REV2"
    if [[ "$ACTION" == freeze ]];then
      python3 -m rev3_confirmatory.campaign freeze \
        --workspace "$WORK" --rounds 60 --warmup 5 --page-size 16 \
        --measurement-seed "$MEAS_SEED" --run-tag "$TAG"
    elif [[ "$ACTION" == preflight ]];then
      python3 -m rev3_confirmatory.campaign preflight --workspace "$WORK"
    elif [[ "$ACTION" == benchmark ]];then
      python3 -m rev3_confirmatory.campaign benchmark \
        --workspace "$WORK" --host 127.0.0.1 --port 9043 \
        --confirm-create CREATE_NEW_DISPOSABLE_R3_KEYSPACES
    elif [[ "$ACTION" == show-flush ]];then
      python3 -m rev3_confirmatory.storage show-flush --workspace "$WORK"
    elif [[ "$ACTION" == storage ]];then
      python3 -m rev3_confirmatory.storage collect --workspace "$WORK" \
        --nodetool-cmd "${NODETOOL_CMD:-docker exec schemalens_iot_cassandra_pipeline-cassandra-1 nodetool}" \
        --out "$NEW_EXP/outputs/rev3_postbenchmark_storage"
    else
      ZIP="$PROJECT/rev3_confirmatory_${CAMPAIGN}_results.zip"
      python3 -m rev3_confirmatory.campaign pack --workspace "$WORK" --out "$ZIP"
    fi
    ;;
 *) echo "Choose generate, freeze, preflight, benchmark or pack" >&2;exit 2;;
esac
