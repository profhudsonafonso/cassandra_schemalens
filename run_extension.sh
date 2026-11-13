#!/bin/zsh
# Steps 2 and 3 requested by the user (2026-11-12), chained after step 1. Each step runs only if the previous passed.
cd ~/Documents/schemalens_v9_cassandra
PY=.venv/bin/python
LOG=runs/v9_generation_001/extension.log
st() { python3 -c "import json;print((json.load(open('runs/v9_generation_001/stages/$1/RESULT.json')) or {}).get('status'))" 2>/dev/null; }
echo "$(date '+%F %T') aguardando passo 1" >> $LOG
while pgrep -f "schemalens_v9.phase6_run" >/dev/null; do sleep 30; done
[ "$(st E21_analysis)" = PASS ] || { echo "$(date '+%F %T') passo 1 não passou; parando" >> $LOG; exit 1; }
echo "$(date '+%F %T') passo 2 (E22)" >> $LOG
$PY -m schemalens_v9.phase7_run >> $LOG 2>&1
[ "$(st E22_criteria_ablations)" = PASS ] || { echo "$(date '+%F %T') E22 não passou; parando" >> $LOG; $PY -c "from schemalens_v9.stages import set_overall; set_overall('FAILED','E22 falhou')"; exit 1; }
for a in 1 2 3; do
  echo "$(date '+%F %T') passo 3 (E23) tentativa $a" >> $LOG
  $PY -m schemalens_v9.phase8_run >> $LOG 2>&1
  [ "$(st E23_full_scale_acceptance)" = PASS ] && break
  sleep 120
done
[ "$(st E23_full_scale_acceptance)" = PASS ] || { $PY -c "from schemalens_v9.stages import set_overall; set_overall('FAILED','E23 falhou')"; exit 1; }
[ -f schemalens_v9/acceptance_v9.py ] && $PY -m schemalens_v9.acceptance_v9 >> $LOG 2>&1
$PY -m schemalens_v9.report >> $LOG 2>&1
$PY -c "from schemalens_v9.stages import set_overall; set_overall('DONE','Passos 1, 2 e 3 concluídos')"
echo "$(date '+%F %T') fim" >> $LOG
