"""Diagnose live Cassandra SSTable storage without rerunning the benchmark.

Run after a failed Stage 9.1 capture to preserve the real reason for failure.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from .experiment import capture_storage


def main(argv=None):
    ap=argparse.ArgumentParser(description='Stage 9.1 nodetool storage diagnostics')
    ap.add_argument('--container',default='schemalens_tpcx_cassandra')
    ap.add_argument('--keyspace',default='sl_tpcx_h_119222012d4e')
    ap.add_argument('--table',action='append',default=None,help='Repeat for each materialized table')
    ap.add_argument('--output',default='outputs/stage91_storage_diagnostic')
    ap.add_argument('--flush',action='store_true',help='Optional: flush the keyspace before reading SSTable metrics')
    a=ap.parse_args(argv)
    tables=a.table or ['ts0_series_raw']
    import re
    if not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{1,99}',a.keyspace) or any(not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{1,99}',t) for t in tables):
        ap.error('Invalid Cassandra keyspace or table identifier')
    result=capture_storage({'tables':tables},a.keyspace,container=a.container,
                           output_dir=Path(a.output).resolve(),flush=a.flush)
    print(json.dumps(result,indent=2,ensure_ascii=False))
    return 0 if result['live_bytes'] is not None else 2

if __name__=='__main__':
    raise SystemExit(main())
