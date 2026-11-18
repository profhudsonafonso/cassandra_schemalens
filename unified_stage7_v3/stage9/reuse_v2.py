"""Stage 9 V3 for benchmarks whose V3 measured space was already measured by V2 under protocol v1 (D-V3-001).

No Cassandra run: after verifying the V3 freeze, records which V2 raw files are re-analysed, with their SHA-256,
and the V3 -> V2 variant mapping. The V2 files are read-only evidence (never modified).

    python -m unified_stage7_v3.stage9.reuse_v2 iot|tpcx_iot
"""
from __future__ import annotations

import datetime as dt
import json
import sys

from ..freeze import HARNESS, paths, verify, verify_prereg
from ..provenance import REPO, sha


def main(argv=None):
    bench = (argv or sys.argv[1:])[0]
    verify(bench, rerun=True)
    reg = verify_prereg(bench)
    if reg['measurement_decision'] != 'REUSE_V2_MEASUREMENTS':
        raise SystemExit(f'ABORT: {bench} is pre-registered as {reg["measurement_decision"]}, not reuse')
    out = paths(bench)['bench'] / 'REUSED_FROM_V2.json'
    files = [f for f in HARNESS[bench] if 'outputs/v1_run1' in f]
    rec = {'benchmark': bench, 'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'rule': 'D-V3-001',
           'v2_raw_files_sha256': {f: sha(REPO / f) for f in files}, 'v3_to_v2_variant': reg['v3_to_v2_variant'],
           'routes_identical_to_v2': reg['routes_identical_to_v2'], 'protocol': 'versao_ajustada/common/protocol_v1.json (V2 campaign)',
           'cassandra_image': json.loads((REPO / 'versao_ajustada/common/protocol_v1.json').read_text())['environment']['cassandra_image']}
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open('x') as f:
        json.dump(rec, f, indent=2, sort_keys=True); f.write('\n')
    print('REUSE RECORDED', out)


if __name__ == '__main__':
    main()
