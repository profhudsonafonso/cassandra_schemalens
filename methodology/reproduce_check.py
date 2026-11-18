"""Verificação offline de reprodutibilidade (sem Cassandra), usada por ./reproduce.sh.

  v2 <bench>  : copia outputs/v1_run1 da V2 para um diretório temporário (sem analysis/), reexecuta o analyze.py
                versionado sobre as latências brutas e compara cada CSV/JSON gerado com o versionado.
  v3          : reexecuta a análise V3 em uma cópia temporária das saídas V3 e compara com as versionadas.

Diferenças aceitas: campos de proveniência que registram hashes de código (`*code_sha256*`) - o código foi
relocado (D-V3-016) - e figuras (PDF carregam data de criação). Números comparados com tolerância relativa 1e-9.
"""
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

METH = Path(__file__).resolve().parent
REPO = METH.parent
PY = sys.executable
V2 = {'iot': ('iot_benchmark/v2_protocolo_v1', 'iot_v1'), 'tpcx_iot': ('tpcx_iot/v2_protocolo_v1', 'tpcx_v1'),
      'tsbs': ('tsbs/v2_protocolo_v1', 'tsbs_v1'), 'rubis': ('rubis/v2_protocolo_v1', 'rubis_v1')}
IGNORE_KEYS = ('code_sha256',)


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def same(a, b, path=''):
    """-> list of differences (empty = identical within tolerance)."""
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            if any(i in str(k) for i in IGNORE_KEYS):
                continue
            if k not in a or k not in b:
                out.append(f'{path}/{k}: chave ausente')
            else:
                out += same(a[k], b[k], f'{path}/{k}')
        return out
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f'{path}: tamanho {len(a)} != {len(b)}']
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in same(x, y, f'{path}[{i}]')]
    na, nb = _num(a), _num(b)
    if na is not None and nb is not None and not isinstance(a, bool):
        if math.isnan(na) and math.isnan(nb):
            return []
        return [] if math.isclose(na, nb, rel_tol=1e-9, abs_tol=1e-12) else [f'{path}: {a} != {b}']
    return [] if a == b else [f'{path}: {a!r} != {b!r}']


def load(p):
    if p.suffix == '.json':
        return json.loads(p.read_text())
    with p.open() as f:
        return list(csv.DictReader(f))


def compare_dirs(ref, new):
    res = {}
    for f in sorted(ref.iterdir()):
        if f.suffix not in ('.json', '.csv') or f.name == 'acceptance.json':
            continue
        g = new / f.name
        res[f.name] = ['arquivo não regenerado'] if not g.exists() else same(load(f), load(g))
    return res


def v2(bench):
    folder, pkg = V2[bench]
    src = REPO / folder / 'outputs/v1_run1'
    if bench == 'rubis' and not any((src / 'v41/raw').glob('*.jsonl.gz')):
        print(f'v2 {bench}: SKIP - traces brutos do RUBiS não estão no clone (2,2 GB; SHA-256 em RAW_SHA256.json)')
        return None
    with tempfile.TemporaryDirectory() as td:
        run = Path(td) / 'v1_run1'
        shutil.copytree(src, run, ignore=shutil.ignore_patterns('analysis', 'outboxes'))
        r = subprocess.run([PY, f'{pkg}/analyze.py', '--run', str(run), '--figs', str(Path(td) / 'figs')],
                           cwd=REPO / folder, capture_output=True, text=True)
        if r.returncode:
            print(f'v2 {bench}: FALHOU ao executar analyze.py\n{r.stderr[-1500:]}')
            return False
        if (REPO / folder / pkg / 'ablation.py').exists():
            r = subprocess.run([PY, f'{pkg}/ablation.py', '--run', str(run)], cwd=REPO / folder, capture_output=True, text=True)
            if r.returncode:
                print(f'v2 {bench}: FALHOU ao executar ablation.py\n{r.stderr[-1500:]}')
                return False
        res = compare_dirs(src / 'analysis', run / 'analysis')
    bad = {k: v for k, v in res.items() if v}
    print(f"v2 {bench}: {len(res) - len(bad)}/{len(res)} arquivos de análise idênticos" + ('' if not bad else f' - DIFERENÇAS: {json.dumps({k: v[:3] for k, v in bad.items()}, ensure_ascii=False)}'))
    return not bad


def v3():
    """Reexecuta a análise V3 com as saídas V3 copiadas para um diretório temporário e compara."""
    sys.path.insert(0, str(METH))
    from unified_stage7_v3 import layout
    rubis_raw = any((layout.REPO / 'rubis/v3_unified/benchmark/run1/v41/raw').glob('*.jsonl.gz'))
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        for b, d in layout.BENCH_DIR.items():
            (tmp / d).mkdir()
            shutil.copytree(layout.REPO / d / 'v3_unified', tmp / d / 'v3_unified',
                            ignore=shutil.ignore_patterns('analysis') if b != 'rubis' else shutil.ignore_patterns('analysis', 'smoke'),
                            symlinks=True)
        code = (f"import sys; sys.path.insert(0, {str(METH)!r})\n"
                "from unified_stage7_v3 import layout\n"
                f"layout.OUT.__class__.__truediv__ = lambda self, rel, _b=__import__('pathlib').Path({str(tmp)!r}): "
                "(lambda parts: (_b / layout.BENCH_DIR[parts[0]] / 'v3_unified').joinpath(*parts[1:]))(__import__('pathlib').PurePosixPath(str(rel)).parts)\n"
                f"layout.DOCS_V3 = __import__('pathlib').Path({str(tmp)!r})\n"
                "import unified_stage7_v3.analysis.run_all as R\nR.DOCS_V3 = layout.DOCS_V3\n"
                "for k in ('analysis/', 'benchmark/'):\n    (layout.OUT / 'tsbs').joinpath(k).mkdir(parents=True, exist_ok=True)\n"
                "R.main()\n")
        for b, d in layout.BENCH_DIR.items():                  # analysis/ starts empty except the pre-benchmark CSV
            (tmp / d / 'v3_unified/analysis').mkdir(exist_ok=True)
            shutil.copy(layout.REPO / d / 'v3_unified/analysis/v2_vs_v3_hbench.csv', tmp / d / 'v3_unified/analysis/')
        if not rubis_raw:                                       # without traces run_all leaves RUBiS as pending
            (tmp / 'rubis/v3_unified/benchmark/run1/manifest.json').unlink()
        r = subprocess.run([PY, '-c', code], cwd=METH, capture_output=True, text=True)
        if r.returncode:
            print(f'v3: FALHOU\n{r.stderr[-2000:]}')
            return False
        ok = True
        for b, d in layout.BENCH_DIR.items():
            if b == 'rubis' and not rubis_raw:
                print('v3 rubis: SKIP - traces brutos do RUBiS não estão no clone (2,2 GB; SHA-256 em RAW_SHA256.json)')
                continue
            res = compare_dirs(layout.REPO / d / 'v3_unified/analysis', tmp / d / 'v3_unified/analysis')
            bad = {k: v for k, v in res.items() if v}
            ok &= not bad
            print(f"v3 {b}: {len(res) - len(bad)}/{len(res)} arquivos de análise idênticos" + ('' if not bad else f' - DIFERENÇAS: {json.dumps({k: v[:3] for k, v in bad.items()}, ensure_ascii=False)}'))
        new_doc = (tmp / 'V3_RESULTS_COMPARISON.md').read_text()
        ref_doc = (layout.DOCS_V3 / 'V3_RESULTS_COMPARISON.md').read_text()
        if not rubis_raw:                                       # compare everything before the RUBiS section
            cut = lambda t: t.split('## rubis', 1)[0]
            new_doc, ref_doc = cut(new_doc), cut(ref_doc)
        doc_ok = new_doc == ref_doc
        print('v3 V3_RESULTS_COMPARISON.md idêntico' + ('' if rubis_raw else ' (sem a seção RUBiS)') + ':', doc_ok)
        return ok and doc_ok


if __name__ == '__main__':
    what = sys.argv[1:] or ['v3', 'iot', 'tpcx_iot', 'tsbs', 'rubis']
    results = [v3() if w == 'v3' else v2(w) for w in what]
    sys.exit(0 if all(r is not False for r in results) else 1)
