"""Stage 9 V3 for TSBS: the V2 protocol-v1 harness (tsbs/v2_protocolo_v1/tsbs_v1/measure.py, unchanged on disk)
driven by the FROZEN V3 pre-registration. Only two things are substituted in memory:
  * the variant list (24 V3 complete deployments + TSBS original baseline + A/A replica), and
  * the route source: each instance runs the frozen V3 route (10_hbench_routes / 05_deployment_vectors),
    no `route_instance` fallback logic.
Everything else (container, cold restarts, flush gate, oracle pre-check and timed oracle check, isolation guard,
counterbalanced order, warm-ups, rounds) is the V2 code path.

    python -m unified_stage7_v3.stage9.tsbs_v3 run              # main campaign -> tsbs/v3_unified/benchmark/run1
    python -m unified_stage7_v3.stage9.tsbs_v3 run --smoke --out DIR
"""
from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys

from ..freeze import paths, verify, verify_prereg
from ..provenance import REPO, dump

from ..layout import PROTOCOL_DIR, V2

TS = V2['tsbs']


def _setup():
    for p in (TS / 'frozen_design', PROTOCOL_DIR, TS / 'tsbs_v1'):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    CP = importlib.import_module('common_paths')
    M = importlib.import_module('measure')
    F2 = importlib.import_module('freeze')
    return CP, M, F2


def harness_registration(reg, CP, smoke=False):
    v2 = json.loads((TS / 'PREREGISTRATION_TSBS_V1.json').read_text())
    variants = reg['variants']
    out = {k: v2[k] for k in ('data', 'instances', 'interval_s', 'decision_metric', 'benchmark')}
    out.update({'status': 'V3_HARNESS_REGISTRATION (derived from tsbs/v3_unified/PREREGISTRATION_V3.json)',
                'variants': variants, 'aa_pairs': reg['aa_pairs'], 'container': reg['container'], 'port': reg['port'],
                'run_tag': reg['run_tag'], 'warmup': reg['warmup'], 'rounds': reg['rounds'],
                'measurement_seed_base': reg['measurement_seed_base'], 'code_sha256': CP.code_hashes(),
                'design_phase': {'frozen_sha256': CP.frozen_hashes()}})
    if smoke:
        keep = [variants[0], variants[2], variants[-2], variants[-1]]
        out.update({'variants': keep, 'container': reg['container'] + '_smoke', 'port': reg['port'] + 100, 'warmup': 1, 'rounds': 2})
    return out


def run(out, smoke=False):
    if smoke:                                           # smoke = harness check BEFORE the freeze (V2 practice); never a result
        from ..plans import plan_for
        reg = plan_for('tsbs')
    else:
        verify('tsbs', rerun=True)                      # ABORT on any hash change (Stage 7/8 rerun compared too)
        reg = verify_prereg('tsbs')
    CP, M, F2 = _setup()
    hreg = harness_registration(reg, CP, smoke)
    if out.exists():
        raise SystemExit(f'ABORT: {out} exists; V3 benchmark outputs are never overwritten')
    out.parent.mkdir(parents=True, exist_ok=True)
    reg_path = out.parent / f'{out.name}_HARNESS_REGISTRATION.json'
    with reg_path.open('x') as f:
        json.dump(hreg, f, indent=2, sort_keys=True, default=str)
    orig_design = F2.design_objects

    def design_objects():
        _deps, plans, writes = orig_design()            # validates the frozen design snapshot hashes (V2 check)
        deps = {}
        for v in hreg['variants']:
            if v['kind'] in ('cover', 'aa_replica'):
                deps[v['deployment_id']] = {'deployment_id': v['deployment_id'], 'candidate_ids': v['candidates'], 'v3_routes': v['v3_routes']}
        for d in deps.values():
            for pid in d['v3_routes'].values():
                if pid not in plans:
                    raise ValueError('frozen V3 route not in the frozen plan catalog: ' + pid)
        return deps, plans, writes
    F2.design_objects = design_objects
    orig_run_query = M.run_query

    def run_query(sess, dep, inst, plans, ds, interval_s, baseline):
        if baseline:
            return orig_run_query(sess, dep, inst, plans, ds, interval_s, baseline)
        from methodology.tsbs.stage5.runtime import bind_read_requests, execute_mock_rows
        pid = dep['v3_routes'][inst['instance_key']]
        plan = plans[pid]
        reqs = bind_read_requests(plan, **inst['params'])
        return execute_mock_rows(plan, [(r, sess.fetch(r)) for r in reqs]), len(reqs), pid
    M.run_query = run_query
    M.PREREG = reg_path
    if smoke:                                           # 1 repetition, tiny window
        proto = M.E.protocol()
        proto['repetitions']['repetitions'] = 1
        M.E.protocol = lambda: proto
    sys.argv = ['measure.py', '--out', str(out)]
    try:
        M.main()
    finally:
        subprocess.run(['docker', 'stop', hreg['container']], capture_output=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['run'])
    ap.add_argument('--smoke', action='store_true')
    ap.add_argument('--out', default=None)
    a = ap.parse_args(argv)
    from pathlib import Path
    out = Path(a.out) if a.out else (paths('tsbs')['root'] / 'smoke' / 'run1' if a.smoke else paths('tsbs')['bench'] / 'run1')
    run(out.resolve(), a.smoke)


if __name__ == '__main__':
    main()
