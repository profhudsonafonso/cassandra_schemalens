"""Caminhos e utilitarios do TSBS v1."""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]             # tsbs/v2_protocolo_v1
REPO_ROOT = ROOT.parents[1]
VA = ROOT.parent
COMMON = REPO_ROOT / 'methodology/protocolo_v1'
SNAP = ROOT / 'frozen_design'
ORIGINAL = REPO_ROOT / 'tsbs/original'
DATA_DIR = ROOT / 'data'
PREREG = ROOT / 'PREREGISTRATION_TSBS_V1.json'
CONTAINER = 'sl_v1_tsbs_cassandra'
PORT = 9063
BASELINE_ID = 'B_tsbs_series_bigint'
AA_SUFFIX = '_aa'
TSBS_COMMIT = '8323e59c74027b108f4ad5ec5d3e498b0101a02e'
# Official TSBS Go generator settings (cpu-only). Scale bounded for an in-memory oracle.
GENERATOR = dict(use_case='cpu-only', seed=20261117, scale=40, log_interval='600s',
                 timestamp_start='2020-01-01T00:00:00Z', timestamp_end='2020-01-02T12:00:00Z', format='cassandra')
INTERVAL_S = 600
INSTANCES = dict(per_template=3, seed=20261118)

for p in (SNAP, COMMON, ROOT / 'tsbs_v1'):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

FROZEN_DIRS = ['config', 'inputs', 'methodology', 'outputs/tsbs_run', 'outputs/tsbs_stage4', 'outputs/tsbs_stage5',
               'outputs/tsbs_stage6', 'outputs/tsbs_stage7', 'outputs/tsbs_stage8']
MEASUREMENT_CODE = ['tsbs_tsdb/tsbs_v1/common_paths.py', 'tsbs_tsdb/tsbs_v1/freeze.py', 'tsbs_tsdb/tsbs_v1/measure.py',
                    'tsbs_tsdb/tsbs_v1/oracle.py', 'common/cassandra_env.py', 'common/protocol_v1.json']


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def dump_x(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf8') as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False, default=str); f.write('\n')


def dump(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False, default=str) + '\n', encoding='utf8')


def csv_x(path, rows, cols=None, mode='x'):
    if not rows:
        raise ValueError('empty csv ' + str(path))
    cols = cols or list(rows[0])
    with Path(path).open(mode, newline='', encoding='utf8') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        if mode != 'a':
            w.writeheader()
        w.writerows(rows)


def read_csv(path):
    with Path(path).open(encoding='utf8', newline='') as f:
        return list(csv.DictReader(f))


def frozen_hashes(base=SNAP):
    base = Path(base); out = {}
    for d in FROZEN_DIRS:
        for p in sorted((base / d).rglob('*')):
            if p.is_file() and '__pycache__' not in p.parts and 'inputs_snapshot' not in p.parts:
                if p.name.endswith('_manifest.json') or p.name == '09_run_manifest.json' or p.name == '01_input_inventory.csv':
                    continue  # contain run timestamps / absolute paths
                out[str(p.relative_to(base))] = sha(p)
    return out


def code_hashes():
    return {rel: sha(_resolve(rel)) for rel in MEASUREMENT_CODE}


def analysis_code_hashes():
    files = sorted((ROOT / 'tsbs_v1').glob('*.py')) + sorted(COMMON.glob('*.py'))
    return {str(p.relative_to(REPO_ROOT)): sha(p) for p in files}


# --- relocated repository (D-V3-016): the V2 hash keys stay the former VA-relative logical names
#     ('common/...', '<former bench folder>/...'); they resolve to the new benchmark-first layout.
def _resolve(rel):
    head, _, rest = rel.partition('/')
    if head == 'common':
        return COMMON / rest
    return ROOT / rest


def relocation_attested(rel_or_path):
    """sha256 recorded for a relocated (path-only) code file by methodology/unified_stage7_v3/RELOCATION_ATTESTATION.json."""
    att = REPO_ROOT / 'methodology/unified_stage7_v3/RELOCATION_ATTESTATION.json'
    if not att.exists():
        return None
    a = json.loads(att.read_text())
    return a.get('attested_files', {}).get(str(rel_or_path)) if a.get('ALL_CHECKS_PASSED') else None


def code_matches_prereg(prereg_code):
    """True when the measurement code equals the pre-registered code, or differs only in files whose relocated
    (path-only) version is attested."""
    now = code_hashes()
    for k, h in prereg_code.items():
        if now.get(k) != h and relocation_attested(str(_resolve(k).relative_to(REPO_ROOT))) != now.get(k):
            return False
    return set(now) == set(prereg_code)
