#!/usr/bin/env bash
# Executa o pipeline completo (etapas 1-8 + etapa 9 offline/dry-run) e os testes.
# Todas as saídas são geradas em outputs/. Não requer Cassandra.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
bash run_tsbs_tsdb_only.sh
bash run_tsbs_stage4.sh
bash run_tsbs_stage5.sh
bash run_tsbs_stage6.sh
bash run_tsbs_stage7.sh
bash run_tsbs_stage8.sh
python3 -m methodology.tsbs.stage9.cli --data inputs/stage9_dev_fixture.csv --all --dry-run --output outputs/tsbs_stage9
for t in activation stage4 stage5 stage6 stage7 stage8 stage9; do
  python3 -m unittest "tests.test_tsbs_$t"
done
