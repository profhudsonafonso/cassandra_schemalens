"""Generate frozen Stage5 artifacts without connecting to Cassandra."""
from __future__ import annotations
import argparse
from pathlib import Path
from .planner import ROOT,generate

def main():
    ap=argparse.ArgumentParser(description='Generate SchemaLens TPCx-IoT Stage5 Cassandra QueryPlans')
    ap.add_argument('--output-dir',type=Path,default=None)
    ns=ap.parse_args()
    m=generate(ROOT,ns.output_dir)
    print(f"Stage5: {m['read_plan_count']} conditional QueryPlans, "
          f"{m['offline_probes_passed']}/{m['offline_probe_count']} offline equivalence probes passed.")
    print('Real Cassandra coverage and stage6 WritePlans: NOT YET VERIFIED')

if __name__=='__main__':main()
