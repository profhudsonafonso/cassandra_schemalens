"""Caminhos, constantes e utilitarios do RUBiS v1 (protocolo comum de execucao v1).

Roda na maquina onde a V9 foi executada (Mac): ali existem o Cassandra 3.11.11 com rubis_full,
o repositorio da V8 (~/Documents/rubis3_claude) e as latencias brutas da V9.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]           # rubis/v2_protocolo_v1
REPO_ROOT = ROOT.parents[1]                          # repository root
VA = ROOT.parent                                     # rubis (former versao_ajustada)
REPO = REPO_ROOT / 'rubis/v9_original'               # RUBiS V9 tree: schemalens_v9/, reused/, runs/
COMMON = REPO_ROOT / 'methodology/protocolo_v1'
V9_RUN = REPO / 'runs/v9_generation_001'
OUT = ROOT / 'outputs'
FROZEN = ROOT / 'frozen'                              # cohort source exported from the 3.11.11 cluster
PREREG = ROOT / 'PREREGISTRATION_RUBIS_V1.json'

SOURCE_CONTAINER = os.environ.get('RUBIS_SOURCE_CONTAINER', 'rubis-schemalens-cassandra')   # 3.11.11 (V9)
SOURCE_PORT = int(os.environ.get('RUBIS_SOURCE_PORT', '9042'))
# Two dedicated nodes loaded with the SAME regenerated data (server mode); RUBIS_ARM selects one per process.
ARMS = {
    'v41': {'container': 'sl_v1_rubis_cassandra', 'port': 9064, 'version': '4.1.12', 'image': None},   # None = protocol image
    'v311': {'container': 'sl_v1_rubis311_cassandra', 'port': 9065, 'version': '3.11.11',
             'image': 'cassandra@sha256:5e13c93dfc49c87dda562e2450b79e2d6f262f0b675ba90bdb382bbaf91bcd33'},
}
ARM = os.environ.get('RUBIS_ARM', 'v41')
CONTAINER = ARMS[ARM]['container']
PORT = ARMS[ARM]['port']
WORKLOADS = ('browsing', 'bidding', 'update10x', 'update100x')
MEASURED, WARMUP, REPS = 10_000, 1_000, 5             # V9/V8-012 frozen request protocol (unchanged)
AA_ID = 'H01_AA'
EXTERNAL = ('NoSE', 'V8_AUTO_H_norm', 'QPG')

for p in (REPO, COMMON, ROOT / 'rubis_v1'):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# V9 frozen design-phase artefacts consumed by the measurement (never modified)
FROZEN_V9 = ['stages/E15_composition/H.json', 'stages/E13_route_synthesis/CANONICAL_WITH_COVER.json',
             'stages/E16_hbench_frozen/HBENCH_FROZEN.json', 'stages/E17_cohort_materialization/COHORT_MANIFEST.json',
             'stages/E02_source_data/SOURCE_COUNTS.json', 'stages/E06_semantic_contract/RESULT.json']
MEASUREMENT_CODE = ['rubis_v9/rubis_v1/common_paths.py', 'rubis_v9/rubis_v1/env.py', 'rubis_v9/rubis_v1/transfer.py',
                    'rubis_v9/rubis_v1/gen_data.py', 'rubis_v9/rubis_v1/oracle_ref.py', 'rubis_v9/rubis_v1/freeze.py',
                    'rubis_v9/rubis_v1/measure.py', 'common/protocol_v1.json']
V9_CODE = ['schemalens_v9/phase5.py', 'schemalens_v9/phase5_run.py', 'schemalens_v9/phase6.py', 'schemalens_v9/isolation.py',
           'schemalens_v9/common.py', 'reused/methodology/protocol_012.py', 'reused/methodology/semantic_replay_011.py',
           'reused/methodology/workload_005.py', 'reused/methodology/baselines_005.py', 'reused/methodology/full_schemalens_012.py',
           'reused/v8_auto_h_norm_ddl.json']


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def read(path, default=None):
    p = Path(path)
    return json.loads(p.read_text(encoding='utf8')) if p.exists() else default


def dump(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False, default=str) + '\n', encoding='utf8')


def dump_x(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf8') as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False, default=str); f.write('\n')


def csv_w(path, rows, cols=None):
    if not rows:
        raise ValueError('empty csv ' + str(path))
    cols = cols or list(rows[0])
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open('w', newline='', encoding='utf8') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore'); w.writeheader(); w.writerows(rows)


def code_hashes():
    return {rel: sha(_resolve(rel)) for rel in MEASUREMENT_CODE}


def v9_hashes():
    return {**{'runs/v9_generation_001/' + rel: sha(V9_RUN / rel) for rel in FROZEN_V9},
            **{rel: sha(REPO / rel) for rel in V9_CODE}}


def use_target_port(port=PORT):
    """Every cassandra.cluster.Cluster created without an explicit port goes to the v1 node.

    The audited V9 code builds Cluster(["127.0.0.1"]) in several places (session(), v8 DDL metadata);
    redirecting the default port avoids editing that code."""
    import cassandra.cluster as CC
    orig = getattr(CC.Cluster, '_v1_orig_init', CC.Cluster.__init__)

    def __init__(self, *a, **k):
        k.setdefault('port', port)
        orig(self, *a, **k)
    CC.Cluster._v1_orig_init = orig
    CC.Cluster.__init__ = __init__
    CC.Cluster._v1_patched = port


def neutralize_v9_state_writes():
    """V9 run_protocol helpers call set_stage() (V9 STATE.json/monitor). The v1 run must not touch the V9 run."""
    from schemalens_v9 import phase6, stages
    noop = lambda *a, **k: None
    phase6.set_stage = noop
    stages.set_stage = noop


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
