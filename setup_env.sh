#!/usr/bin/env bash
# Cria o ambiente Python único do repositório em ./.venv (Python 3.12) com as versões fixadas.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
PYTHON="${PYTHON:-python3}"
[[ -x .venv/bin/python ]] || "$PYTHON" -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements-lock.txt   # versões exatas (requirements.txt lista as diretas)
.venv/bin/python -c "import cassandra, numpy, matplotlib, yaml, pulp, pytest; print('ambiente ok: python', __import__('sys').version.split()[0], '| cassandra-driver', cassandra.__version__, '| pulp', pulp.__version__)"
.venv/bin/pip freeze > .venv/FREEZE.txt
