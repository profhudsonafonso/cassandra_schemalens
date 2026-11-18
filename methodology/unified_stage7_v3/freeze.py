"""V3 freeze: SHA-256 of V3 code, inputs, Stage 7/8 outputs, scenario and the measurement protocol/harness;
pre-registration; verification that ABORTs on any change.

    python -m unified_stage7_v3.freeze freeze <bench>
    python -m unified_stage7_v3.freeze verify <bench>
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from .provenance import REPO, code_hashes, sha

from .layout import METH, OUT, relocate
PROTOCOL = ['methodology/protocolo_v1/protocol_v1.json', 'methodology/protocolo_v1/cassandra_env.py',
            'methodology/protocolo_v1/slstats.py', 'methodology/protocolo_v1/method_eval.py']
HARNESS = {
    'iot': ['iot_benchmark/v2_protocolo_v1/PREREGISTRATION_IOT_V1.json',
            'iot_benchmark/v2_protocolo_v1/outputs/v1_run1/raw_latencies.csv',
            'iot_benchmark/v2_protocolo_v1/outputs/v1_run1/manifest.json'],
    'tpcx_iot': ['tpcx_iot/v2_protocolo_v1/PREREGISTRATION_TPCX_V1.json',
                 'tpcx_iot/v2_protocolo_v1/outputs/v1_run1/raw_reads.csv',
                 'tpcx_iot/v2_protocolo_v1/outputs/v1_run1/manifest.json'],
    'tsbs': ['tsbs/v2_protocolo_v1/tsbs_v1/common_paths.py', 'tsbs/v2_protocolo_v1/tsbs_v1/freeze.py',
             'tsbs/v2_protocolo_v1/tsbs_v1/measure.py', 'tsbs/v2_protocolo_v1/tsbs_v1/oracle.py',
             'tsbs/v2_protocolo_v1/data/tsbs_cpu_only_cassandra_double.txt.gz',
             'tsbs/v2_protocolo_v1/data/query_instances.json'],
    'rubis': ['rubis/v2_protocolo_v1/rubis_v1/common_paths.py', 'rubis/v2_protocolo_v1/rubis_v1/env.py',
              'rubis/v2_protocolo_v1/rubis_v1/measure.py', 'rubis/v2_protocolo_v1/rubis_v1/oracle_ref.py',
              'rubis/v2_protocolo_v1/frozen/cohort_source.pkl', 'rubis/v2_protocolo_v1/frozen/ORACLE_REF.pkl',
              'rubis/v2_protocolo_v1/frozen/TRANSFER_MANIFEST.json',
              'rubis/v9_original/schemalens_v9/phase5.py', 'rubis/v9_original/schemalens_v9/phase6.py', 'rubis/v9_original/schemalens_v9/routes.py', 'rubis/v9_original/schemalens_v9/phase2.py'],
}
DETERMINISTIC = ['00_input_hashes.json', '01_scenario.json', '02_query_read_vectors.json', '03_write_vectors.json',
                 '04_candidate_footprints.json', '05_deployment_vectors.json', '06_complete_deployments.json',
                 '07_pareto_front.json', '08_diversity_selection.json', '09_hbench_v3.json', '10_hbench_routes.json',
                 '11_selection_trace.json']


class Abort(SystemExit):
    pass


def paths(bench, root=OUT):
    b = root / bench
    return {'root': b, 'pre': b / 'prebenchmark', 'bench': b / 'benchmark', 'frozen': b / 'prebenchmark' / 'FROZEN.sha256',
            'prereg': b / 'PREREGISTRATION_V3.json', 'scenario': b / 'V3_PREBENCHMARK_SCENARIO.json'}


def collect(bench, root=OUT):
    p = paths(bench, root)
    inputs = json.loads((p['pre'] / '00_input_hashes.json').read_text())
    return {
        'v3_code': code_hashes(),
        'stage7_inputs_live': {k: sha(REPO / relocate(k)) for k in sorted(inputs)},
        'stage7_8_outputs': {f.name: sha(f) for f in sorted(p['pre'].glob('[01][0-9]_*.json'))},
        'scenario': sha(p['scenario']),
        'protocol': {k: sha(REPO / k) for k in PROTOCOL},
        'measurement_harness_and_data': {k: sha(REPO / k) for k in HARNESS[bench]},
    }


def _diff(a, b, prefix=''):
    out = []
    for k in sorted(set(a) | set(b)):
        if isinstance(a.get(k), dict) or isinstance(b.get(k), dict):
            out += _diff(a.get(k) or {}, b.get(k) or {}, f'{prefix}{k}/')
        elif a.get(k) != b.get(k):
            out.append(f'{prefix}{k}')
    return out


def rerun_hashes(bench, keep_00=False):
    """Re-run Stage 7/8 in a fresh process into a temp dir; return the hashes of the deterministic outputs.
    keep_00: also return the parsed 00_input_hashes (its keys are file paths, compared after relocation)."""
    with tempfile.TemporaryDirectory() as td:
        subprocess.run([sys.executable, '-m', 'unified_stage7_v3.run_prebenchmark', bench, '--out', str(Path(td) / 'pre')],
                       cwd=METH, check=True, capture_output=True)
        h = {n: sha(Path(td) / 'pre' / n) for n in DETERMINISTIC}
        return (h, json.loads((Path(td) / 'pre' / '00_input_hashes.json').read_text())) if keep_00 else h


def _relocated(d):
    return {relocate(k): v for k, v in d.items()}


def verify(bench, root=OUT, rerun=False):
    p = paths(bench, root)
    if not p['frozen'].exists():
        raise Abort(f'ABORT: {bench} HBench V3 is not frozen ({p["frozen"]} missing)')
    frozen = json.loads(p['frozen'].read_text())
    inputs = json.loads((p['pre'] / '00_input_hashes.json').read_text())
    if frozen['stage7_inputs_live'] != inputs:
        raise Abort(f'ABORT: {bench} frozen input hashes disagree with 00_input_hashes.json')
    now = collect(bench, root)
    ref = {k: frozen[k] for k in now}
    # Relocated repository (D-V3-016): frozen keys are former paths -> compare at the relocated paths; the V3 code
    # is accepted either as frozen or as attested by relocation.RELOCATION_ATTESTATION.json (path-only change).
    for sec in ('protocol', 'measurement_harness_and_data'):
        ref[sec] = _relocated(ref[sec])
    from .relocation import attested_code, attested_files
    if ref['v3_code'] != now['v3_code'] and attested_code() == now['v3_code']:
        ref['v3_code'] = now['v3_code']
    att = attested_files()
    for sec in ('protocol', 'measurement_harness_and_data'):
        for k, h in now[sec].items():
            if ref[sec].get(k) != h and att.get(k) == h:          # relocated (path-only) code file, attested
                ref[sec][k] = h
    d = _diff(ref, now)
    if d:
        raise Abort(f'ABORT: {bench} hash changed after freeze: ' + ', '.join(d[:10]))
    if sha(p['prereg']) != frozen['preregistration_sha256']:
        raise Abort(f'ABORT: {bench} preregistration changed after freeze')
    if rerun:
        rh, r00 = rerun_hashes(bench, keep_00=True)
        fd = dict(frozen['determinism_rerun'])
        if rh['00_input_hashes.json'] != fd['00_input_hashes.json'] and _relocated(inputs) == _relocated(r00):
            fd['00_input_hashes.json'] = rh['00_input_hashes.json']      # same inputs and hashes, relocated keys
        if rh != fd:
            raise Abort(f'ABORT: {bench} Stage 7/8 rerun differs: ' + ', '.join(_diff(fd, rh)))
    return True


def freeze(bench, plan: dict, root=OUT, rerun=True):
    p = paths(bench, root)
    if p['frozen'].exists():
        raise Abort(f'ABORT: {bench} already frozen; freezing is done once')
    if p['bench'].exists() and any(p['bench'].rglob('*')):
        raise Abort(f'ABORT: {bench} benchmark outputs already exist; HBench must be frozen before any V3 benchmark output')
    h = collect(bench, root)
    inputs = json.loads((p['pre'] / '00_input_hashes.json').read_text())
    if h['stage7_inputs_live'] != inputs:
        raise Abort(f'ABORT: {bench} Stage 7 inputs changed since the prebenchmark run: ' + ', '.join(_diff(inputs, h['stage7_inputs_live'])))
    det = rerun_hashes(bench) if rerun else None
    if det is not None:
        official = {n: sha(p['pre'] / n) for n in DETERMINISTIC}
        if det != official:
            raise Abort(f'ABORT: {bench} Stage 7/8 is not deterministic: ' + ', '.join(_diff(official, det)))
    man = json.loads((p['pre'] / '12_frozen_manifest.json').read_text())
    reg = {'status': 'PREREGISTERED_BEFORE_V3_MEASUREMENT', 'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
           'benchmark': bench, 'policy': 'SCHEMALENS_UNIFIED_STAGE7_V3', 'hbench_v3_ranked': man['hbench_ids'],
           'selection_rule': man['selection_rule'], 'firewall': {'selection_uses_measured_results': False, 'latency_files_read': [],
                                                                 'benchmark_outputs_read': []},
           **plan}
    from .provenance import dump
    dump(p['prereg'], reg)
    rec = {**h, 'determinism_rerun': det, 'preregistration_sha256': sha(p['prereg']),
           'frozen_utc': reg['created_utc'], 'rule': 'any later difference => ABORT (verify() is called by every V3 Stage 9 entry point)'}
    with p['frozen'].open('x') as f:
        json.dump(rec, f, indent=2, sort_keys=True); f.write('\n')
    return rec


def verify_prereg(bench, root=OUT):
    p = paths(bench, root)
    frozen = json.loads(p['frozen'].read_text())
    if sha(p['prereg']) != frozen['preregistration_sha256']:
        raise Abort(f'ABORT: {bench} preregistration changed after freeze')
    return json.loads(p['prereg'].read_text())


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['freeze', 'verify'])
    ap.add_argument('bench')
    a = ap.parse_args(argv)
    if a.cmd == 'verify':
        verify(a.bench, rerun=True); verify_prereg(a.bench)
        print('VERIFIED', a.bench)
    else:
        from .plans import plan_for
        rec = freeze(a.bench, plan_for(a.bench))
        print('FROZEN', a.bench, rec['frozen_utc'])


if __name__ == '__main__':
    main()
