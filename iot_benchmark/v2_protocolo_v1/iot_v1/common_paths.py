"""Caminhos, importacao do codigo auditado (snapshot B) e utilitarios."""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]            # iot_benchmark/v2_protocolo_v1
REPO_ROOT = ROOT.parents[1]                          # repository root
VA = ROOT.parent                                     # iot_benchmark (former versao_ajustada)
COMMON = REPO_ROOT / 'methodology/protocolo_v1'
SNAP = ROOT / 'frozen_design_B'
ORIGINAL_B = Path('/home/hudson/Documents/schemalens_cassandra/schemalens_rev3_new_java_B_20261105_193618/'
                  'java_work/runs/v3_20261105T223620Z_pid3665162/schemalens_rev3_ts5_contract_validated_B')
REV3 = SNAP / 'outputs/v4_rev3_planaware_prebenchmark'
REV2 = SNAP / 'outputs/v4_rev2_prebenchmark'
PREREG = ROOT / 'PREREGISTRATION_IOT_V1.json'
CONTAINER = 'sl_v1_iot_cassandra'
PORT = 9061
LABEL_ORIG = 'BASELINE_ORIGINAL'
LABEL_OPT = 'BASELINE_Q8_OPTIMIZED'

for p in (SNAP, SNAP / 'SchemaLens_V4_Rev3_PlanAware', SNAP / 'SchemaLens_V4_Rev2_Confirmatory', COMMON):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

EXPECTED_REV3_CODE = {
    'core.py': '420caac9e8626c4f20435a80abecb0b958f2f73bd8ee883cfd9b512fd1a88518',
    'prebenchmark.py': '5ebedc07f231f863eb96946e3195c710d12bb6c45b9f108a6e6e67e445268fad',
    'runtime.py': '7cbb1b95657dc9bd432a8b568df92b6b7048f85597c0513c97d948749bafebf4',
}

# Artefatos congelados da fase de projeto que a medicao consome.
FROZEN_FILES = [
    'phase_a1/outputs/normalized_double_readings.csv',
    'phase_a1/outputs/query_instances_pilot.json',
    'phase_a1/outputs/activation_ts_only.json',
    'phase_a1/vendor/tsdb_base/config/tsdb_activation_rules.json',
    'phase_a1/vendor/tsdb_base/config/temporal_modifier_rules.json',
    'phase_a1/vendor/tsdb_base/config/tsdb_family_catalog.json',
    'phase_a1/vendor/tsdb_base/methodology/common/rule_engine.py',
    'phase_a1/core/bridge.py',
    'config/pipeline_policy.json',
    'outputs/stage4/10_candidate_specs.json',
    'outputs/v4_rev2_prebenchmark/stage4/10_v4_candidate_specs.json',
    'outputs/v4_rev2_prebenchmark/stage5/15_cross_query_coverage.json',
    'outputs/v4_rev2_prebenchmark/stage7/21_storage_proxies.json',
    'outputs/v4_rev2_prebenchmark/stage7/22_write_proxies.json',
    'outputs/v4_rev2_prebenchmark/stage7/23_read_proxies_rev2.json',
    'outputs/v4_rev2_prebenchmark/stage8/28_hbench_v4_rev2_frozen.json',
    'outputs/v4_rev3_planaware_prebenchmark/stage5/15_executable_plan_catalog.json',
    'outputs/v4_rev3_planaware_prebenchmark/stage8/28_hbench_v4_rev3_frozen.json',
    'outputs/v4_rev3_planaware_prebenchmark/stage8/29_all_compatible_covers.json',
    'outputs/v4_rev3_planaware_prebenchmark/stage8/manifest.json',
    'SchemaLens_V4_Rev3_PlanAware/rev3/core.py',
    'SchemaLens_V4_Rev3_PlanAware/rev3/prebenchmark.py',
    'SchemaLens_V4_Rev3_PlanAware/rev3/runtime.py',
    'SchemaLens_V4_Rev2_Confirmatory/v4_auto/selector.py',
    'SchemaLens_V4_Rev2_Confirmatory/v4_rev2/runtime_helpers.py',
    'SchemaLens_V4_Rev2_Confirmatory/v4_rev2/cost.py',
    'iot_pipeline/shared.py', 'iot_pipeline/stage9.py', 'iot_pipeline/offline.py',
    'iot_pipeline/native_reference_v3.py', 'iot_pipeline/reference_baseline.py',
]


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def dump_x(path, obj):
    """Exclusive write (never overwrite evidence)."""
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


MEASUREMENT_CODE = ['iot_benchmark_tsdb/iot_v1/common_paths.py', 'iot_benchmark_tsdb/iot_v1/freeze.py',
                    'iot_benchmark_tsdb/iot_v1/measure.py', 'common/cassandra_env.py', 'common/protocol_v1.json']


def code_hashes():
    """Hash of the code that can affect what is measured (analysis plan is fixed by protocol_v1.json)."""
    return {rel: sha(_resolve(rel)) for rel in MEASUREMENT_CODE}


def analysis_code_hashes():
    files = sorted((ROOT / 'iot_v1').glob('*.py')) + sorted(COMMON.glob('*.py'))
    return {str(p.relative_to(REPO_ROOT)): sha(p) for p in files}


def frozen_hashes(base=SNAP):
    return {rel: sha(Path(base) / rel) for rel in FROZEN_FILES}


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
