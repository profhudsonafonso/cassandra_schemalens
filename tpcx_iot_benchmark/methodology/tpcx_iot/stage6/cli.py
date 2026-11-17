"""Run Stage6 source verification, WritePlan generation and offline correctness suite."""
from __future__ import annotations
import argparse
from pathlib import Path
from .planner import ROOT,generate

def main():
    p=argparse.ArgumentParser(description='SchemaLens TPCx-IoT Cassandra Stage 6 WritePlans')
    p.add_argument('--output-dir',type=Path,default=None)
    ns=p.parse_args()
    result=generate(ROOT,ns.output_dir)
    print(f"Stage6: {result['write_plan_count']} conditional WritePlans, "
          f"{result['offline_probes_passed']}/{result['offline_probe_count']} offline checks passed")
    print('Not executed on Cassandra. No durable distributed maintenance or HBench claim.')

if __name__=='__main__':main()
