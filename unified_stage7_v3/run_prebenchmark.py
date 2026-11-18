"""python -m unified_stage7_v3.run_prebenchmark <iot|tpcx_iot|tsbs|rubis> [--out DIR]

Runs Stage 7 V3 + Stage 8 V3 for one benchmark under the firewall (one process per benchmark: each frozen
snapshot ships its own `methodology` package). Refuses to overwrite a frozen prebenchmark directory."""
from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

from .guards import firewall
from .provenance import REPO, dump, run_prebenchmark

ADAPTERS = {'iot': 'iot', 'tpcx_iot': 'tpcx_iot', 'tsbs': 'tsbs', 'rubis': 'rubis'}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('bench', choices=sorted(ADAPTERS))
    ap.add_argument('--out', default=None)
    ap.add_argument('--force-scratch', action='store_true', help='allow overwriting (only for non-frozen scratch dirs)')
    a = ap.parse_args(argv)
    out = Path(a.out) if a.out else REPO / 'outputs_v3' / a.bench / 'prebenchmark'
    if (out / 'FROZEN.sha256').exists():
        sys.exit(f'ABORT: {out} is frozen; V3 prebenchmark outputs are never overwritten')
    mod = importlib.import_module(f'.adapters.{ADAPTERS[a.bench]}', __package__)
    with firewall():
        canon = mod.build()
        man, complete, tr = run_prebenchmark(a.bench, canon, out)
    dump(out.parent / 'V3_PREBENCHMARK_SCENARIO.json', canon['scenario'])
    print(json.dumps({'bench': a.bench, 'complete': len(complete), 'pareto': len(tr['pareto_front']),
                      'rule': tr['rule'], 'hbench': tr['selected']}, indent=1))


if __name__ == '__main__':
    main()
