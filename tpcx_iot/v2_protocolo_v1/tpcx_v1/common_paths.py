"""Caminhos, importacao do codigo congelado do TPCx-IoT e utilitarios."""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]            # tpcx_iot/v2_protocolo_v1
REPO_ROOT = ROOT.parents[1]
VA = ROOT.parent
COMMON = REPO_ROOT / 'methodology/protocolo_v1'
SNAP = ROOT / 'frozen_design'
ORIGINAL = REPO_ROOT / 'tpcx_iot/original'
PREREG = ROOT / 'PREREGISTRATION_TPCX_V1.json'
CONTAINER = 'sl_v1_tpcx_cassandra'
PORT = 9062
BASELINE_ID = 'b_tpcx_logical_raw'
AA_SUFFIX = '_aa'
# Workload definition kept from Stage 9.1 (scale is NOT changed by the v1 protocol)
WORKLOAD = dict(stations=2, sensors=3, seed_steps=130, bench_steps=500, read_threads=2,
                read_operations=120, tick_ms=1000, pad_bytes=900, warmup_reads=10)

for p in (SNAP, COMMON):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

FROZEN_DIRS = ['config', 'inputs', 'methodology', 'outputs/tpcx_iot_stage1_3', 'outputs/tpcx_iot_stage4',
               'outputs/tpcx_iot_stage5', 'outputs/tpcx_iot_stage6', 'outputs/tpcx_iot_stage7',
               'outputs/tpcx_iot_stage8']
MEASUREMENT_CODE = ['tpcx_iot_benchmark/tpcx_v1/common_paths.py', 'tpcx_iot_benchmark/tpcx_v1/freeze.py',
                    'tpcx_iot_benchmark/tpcx_v1/measure.py', 'common/cassandra_env.py', 'common/protocol_v1.json']


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
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore'); w.writeheader(); w.writerows(rows)


def read_csv(path):
    with Path(path).open(encoding='utf8', newline='') as f:
        return list(csv.DictReader(f))


def frozen_hashes(base=SNAP):
    base = Path(base); out = {}
    for d in FROZEN_DIRS:
        for p in sorted((base / d).rglob('*')):
            if p.is_file() and '__pycache__' not in p.parts:
                out[str(p.relative_to(base))] = sha(p)
    return out


def code_hashes():
    return {rel: sha(_resolve(rel)) for rel in MEASUREMENT_CODE}


def analysis_code_hashes():
    files = sorted((ROOT / 'tpcx_v1').glob('*.py')) + sorted(COMMON.glob('*.py'))
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
