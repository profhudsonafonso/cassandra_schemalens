#!/bin/zsh
# Detached master for steps 1-3 (user request 2026-11-12). Survives the closing of the SSH session.
cd ~/Documents/schemalens_v9_cassandra
LOG=runs/v9_generation_001/extension.log
echo "$(date '+%F %T') master iniciado (pid $$)" >> $LOG
.venv/bin/python -m schemalens_v9.phase6_run >> runs/v9_generation_001/phase6.stdout.log 2>&1
./run_extension.sh
