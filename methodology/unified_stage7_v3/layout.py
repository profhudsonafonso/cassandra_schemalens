"""Single source of repository paths for the V3 code (benchmark-first layout, D-V3-016).

    <repo>/methodology/{protocolo_v1, unified_stage7_v3, docs, artigos}
    <repo>/<benchmark>/{original | v9_original, v2_protocolo_v1, v3_unified}

`OUT / '<bench>/...'` resolves to `<repo>/<benchmark folder>/v3_unified/...` so that code written against the
former `outputs_v3/<bench>/...` layout keeps one meaning. RELOCATION maps every former repo-relative path to the
new one; it is used to verify the historical V3 freeze (recorded with the former paths) against the relocated files.
"""
from __future__ import annotations

import sys
from pathlib import Path, PurePosixPath

PKG = Path(__file__).resolve().parent
METH = PKG.parent
REPO = METH.parent
PROTOCOL_DIR = METH / 'protocolo_v1'
DOCS_V3 = METH / 'docs' / 'v3'
BENCH_DIR = {'iot': 'iot_benchmark', 'tpcx_iot': 'tpcx_iot', 'tsbs': 'tsbs', 'rubis': 'rubis'}
V2 = {'iot': REPO / 'iot_benchmark/v2_protocolo_v1', 'tpcx_iot': REPO / 'tpcx_iot/v2_protocolo_v1',
      'tsbs': REPO / 'tsbs/v2_protocolo_v1', 'rubis': REPO / 'rubis/v2_protocolo_v1'}
RUBIS_V9 = REPO / 'rubis/v9_original'
V9_RUN = RUBIS_V9 / 'runs/v9_generation_001'

# former repo-relative prefix -> new repo-relative prefix (longest prefixes first)
RELOCATION = [
    ('versao_ajustada/iot_benchmark_tsdb/', 'iot_benchmark/v2_protocolo_v1/'),
    ('versao_ajustada/tpcx_iot_benchmark/', 'tpcx_iot/v2_protocolo_v1/'),
    ('versao_ajustada/tsbs_tsdb/', 'tsbs/v2_protocolo_v1/'),
    ('versao_ajustada/rubis_v9/', 'rubis/v2_protocolo_v1/'),
    ('versao_ajustada/common/', 'methodology/protocolo_v1/'),
    ('versao_ajustada/artigo_overleaf/', 'methodology/artigos/v2_protocolo_v1/'),
    ('outputs_v3/iot/', 'iot_benchmark/v3_unified/'),
    ('outputs_v3/tpcx_iot/', 'tpcx_iot/v3_unified/'),
    ('outputs_v3/tsbs/', 'tsbs/v3_unified/'),
    ('outputs_v3/rubis/', 'rubis/v3_unified/'),
    ('unified_stage7_v3/', 'methodology/unified_stage7_v3/'),
    ('docs/V3_', 'methodology/docs/v3/V3_'),
    ('artigo_overleaf_v3/', 'methodology/artigos/v3_unified/'),
    ('runs/', 'rubis/v9_original/runs/'),
    ('schemalens_v9/', 'rubis/v9_original/schemalens_v9/'),
    ('reused/', 'rubis/v9_original/reused/'),
    ('iot_benchmark_tsdb/', 'iot_benchmark/original/'),
    ('tpcx_iot_benchmark/', 'tpcx_iot/original/'),
    ('SchemaLens_TSBS_TSDB_Only/', 'tsbs/original/'),
]


def relocate(old: str) -> str:
    """Former repo-relative path -> current repo-relative path (unchanged if no prefix matches)."""
    for a, b in RELOCATION:
        if old.startswith(a):
            return b + old[len(a):]
    return old


class _V3Root:
    """`OUT / 'tsbs/benchmark/run1'` -> <repo>/tsbs/v3_unified/benchmark/run1."""

    def __truediv__(self, rel):
        parts = PurePosixPath(str(rel)).parts
        base = REPO / BENCH_DIR[parts[0]] / 'v3_unified'
        return base.joinpath(*parts[1:]) if len(parts) > 1 else base

    def __repr__(self):
        return '<V3 outputs: <repo>/<benchmark>/v3_unified>'


OUT = _V3Root()


def ensure_paths(*paths):
    for p in paths:
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))


def ensure_v9():
    """schemalens_v9 / reused are top-level packages of the RUBiS V9 tree."""
    ensure_paths(RUBIS_V9)


def ensure_protocol():
    ensure_paths(PROTOCOL_DIR)
