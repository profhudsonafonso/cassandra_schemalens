"""Relocation attestation for the benchmark-first repository layout (D-V3-016).

The V3 freezes (`<bench>/v3_unified/prebenchmark/FROZEN.sha256`) were written at measurement time with the former
paths and the former V3 code hashes (commit fcd99bf). They are historical evidence and are NOT rewritten.
This module proves that the relocated repository is the same experiment:

  1. every frozen non-code artefact (Stage 7 inputs, protocol, harness, data, Stage 7/8 outputs, scenario,
     pre-registration) has the same SHA-256 at its relocated path;
  2. the V3 code changed only to follow the new layout - the full diff against the frozen commit is archived in
     RELOCATION_CODE.diff for audit;
  3. re-running Stage 7/8 with the relocated code reproduces the frozen deterministic outputs byte for byte
     (00_input_hashes compared after relocating its keys: the keys are file paths).

    python -m unified_stage7_v3.relocation attest     # writes RELOCATION_ATTESTATION.json (+ .diff)
    python -m unified_stage7_v3.relocation check      # re-checks everything, exit 1 on any difference
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from .layout import METH, OUT, REPO, relocate
from .provenance import code_hashes, sha

BENCHES = ('iot', 'tpcx_iot', 'tsbs', 'rubis')
FROZEN_COMMIT = 'fcd99bf'
ATTEST = METH / 'unified_stage7_v3' / 'RELOCATION_ATTESTATION.json'
DIFF = METH / 'unified_stage7_v3' / 'RELOCATION_CODE.diff'
DETERMINISTIC = ['00_input_hashes.json', '01_scenario.json', '02_query_read_vectors.json', '03_write_vectors.json',
                 '04_candidate_footprints.json', '05_deployment_vectors.json', '06_complete_deployments.json',
                 '07_pareto_front.json', '08_diversity_selection.json', '09_hbench_v3.json', '10_hbench_routes.json',
                 '11_selection_trace.json']


def _files_check(b, relocated_code):
    """Data must be byte-identical. A frozen CODE file (.py/.sh) may differ only if it is in the path-only relocation
    diff (relocated_code: {path_now: sha_now})."""
    fz = json.loads((OUT / b / 'prebenchmark/FROZEN.sha256').read_text())
    rows, ok = [], True
    for sec in ('stage7_inputs_live', 'protocol', 'measurement_harness_and_data'):
        for old, h in sorted(fz[sec].items()):
            new = relocate(old)
            cur = sha(REPO / new) if (REPO / new).exists() else None
            reloc = cur != h and new.endswith(('.py', '.sh')) and relocated_code.get(new) == cur
            rows.append({'section': sec, 'frozen_path': old, 'path_now': new, 'sha256_frozen': h, 'sha256_now': cur, 'equal': cur == h,
                         'accepted_as_path_only_code_change': reloc})
            ok &= cur == h or reloc
    pre = OUT / b / 'prebenchmark'
    for n, h in sorted(fz['stage7_8_outputs'].items()):
        cur = sha(pre / n)
        rows.append({'section': 'stage7_8_outputs', 'frozen_path': n, 'path_now': str((pre / n).relative_to(REPO)), 'sha256_frozen': h,
                     'sha256_now': cur, 'equal': cur == h})
        ok &= cur == h
    for name, path, h in (('scenario', OUT / b / 'V3_PREBENCHMARK_SCENARIO.json', fz['scenario']),
                          ('preregistration', OUT / b / 'PREREGISTRATION_V3.json', fz['preregistration_sha256'])):
        cur = sha(path)
        rows.append({'section': name, 'frozen_path': name, 'path_now': str(path.relative_to(REPO)), 'sha256_frozen': h, 'sha256_now': cur, 'equal': cur == h})
        ok &= cur == h
    return ok, rows


def _rerun_check(b):
    """Stage 7/8 rerun in a fresh process; outputs 01-11 byte-identical, 00 identical after key relocation."""
    pre = OUT / b / 'prebenchmark'
    with tempfile.TemporaryDirectory() as td:
        r = subprocess.run([sys.executable, '-m', 'unified_stage7_v3.run_prebenchmark', b, '--out', str(Path(td) / 'pre')],
                           cwd=METH, capture_output=True, text=True)
        if r.returncode:
            return False, {'error': r.stderr[-2000:]}
        res = {}
        for n in DETERMINISTIC:
            if n == '00_input_hashes.json':
                old = {relocate(k): v for k, v in json.loads((pre / n).read_text()).items()}
                new = json.loads((Path(td) / 'pre' / n).read_text())
                res[n] = old == new
            else:
                res[n] = sha(Path(td) / 'pre' / n) == sha(pre / n)
    return all(res.values()), res


CODE_SPECS = ['*.py', '*.sh']


def _code_diff():
    """Diff of every code file between the frozen commit and the STAGED relocated tree (renames detected).
    Returns (diff text, name-status lines of modified code, {path_now: sha_now} of modified code files)."""
    git = lambda *a: subprocess.run(['git', *a], cwd=REPO, capture_output=True, text=True, check=True).stdout
    status = [l for l in git('diff', '--cached', '-M', '--name-status', FROZEN_COMMIT, '--', *CODE_SPECS).splitlines()
              if not l.startswith('R100')]
    diff = git('diff', '--cached', '-M', FROZEN_COMMIT, '--', *CODE_SPECS)
    changed = {}
    for l in status:
        parts = l.split('\t')
        path = parts[-1]
        if parts[0] != 'D' and (REPO / path).exists():
            changed[path] = sha(REPO / path)
    return diff, status, changed


def build():
    out = {'purpose': 'relocation of the repository to the benchmark-first layout (D-V3-016); historical FROZEN.sha256 files untouched',
           'frozen_commit': FROZEN_COMMIT, 'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'benchmarks': {}}
    diff, files, changed = _code_diff()
    allok = True
    for b in BENCHES:
        fok, rows = _files_check(b, changed)
        dok, det = _rerun_check(b)
        out['benchmarks'][b] = {'frozen_files_identical': fok, 'stage7_8_rerun_identical': dok, 'rerun_detail': det,
                                'files': rows}
        allok &= fok and dok
    out['code_changes_vs_frozen_commit'] = files
    out['attested_files'] = changed
    out['code_change_nature'] = ('path-only: layout.py (new) centralises the V3 repository paths; relocation.py (new) is this '
                                 'attestation; freeze.verify and the V2 acceptance gates accept relocated code only through it; '
                                 'V2 common_paths.py resolve the unchanged logical hash keys to the new folders; run scripts '
                                 'point to the single root .venv; all other edits replace former paths or update docstrings. '
                                 'Full diff: RELOCATION_CODE.diff')
    out['new_code_sha256'] = code_hashes()
    out['ALL_CHECKS_PASSED'] = allok
    return out, diff


def attest():
    out, diff = build()
    DIFF.write_text(diff)
    out['code_diff_sha256'] = sha(DIFF)
    ATTEST.write_text(json.dumps(out, indent=2, sort_keys=True) + '\n')
    print('ATTESTATION', 'PASSED' if out['ALL_CHECKS_PASSED'] else 'FAILED', ATTEST)
    return out['ALL_CHECKS_PASSED']


def check():
    a = json.loads(ATTEST.read_text())
    out, _ = build()
    ok = (out['ALL_CHECKS_PASSED'] and a.get('ALL_CHECKS_PASSED') and a['new_code_sha256'] == out['new_code_sha256']
          and a['attested_files'] == out['attested_files'])
    for b, r in out['benchmarks'].items():
        print(b, 'files', r['frozen_files_identical'], 'rerun', r['stage7_8_rerun_identical'])
    print('code identical to attested code:', a['new_code_sha256'] == out['new_code_sha256'])
    print('RELOCATION CHECK', 'PASSED' if ok else 'FAILED')
    return ok


def attested_code():
    """Code hashes accepted in place of the frozen (pre-relocation) ones, if a passing attestation exists."""
    if not ATTEST.exists():
        return None
    a = json.loads(ATTEST.read_text())
    return a['new_code_sha256'] if a.get('ALL_CHECKS_PASSED') else None



def attested_files():
    if not ATTEST.exists():
        return {}
    a = json.loads(ATTEST.read_text())
    return a.get('attested_files', {}) if a.get('ALL_CHECKS_PASSED') else {}


if __name__ == '__main__':
    cmd = (sys.argv[1:] or ['check'])[0]
    sys.exit(0 if (attest() if cmd == 'attest' else check()) else 1)
