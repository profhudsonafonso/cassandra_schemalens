"""CLI for Stage7 pre-benchmark physical cost proxies."""
from __future__ import annotations
import argparse
from pathlib import Path
from methodology.tpcx_iot.stage4.candidates import ROOT
from .estimator import generate

def main() -> None:
    parser=argparse.ArgumentParser(description='SchemaLens TPCx-IoT Cassandra Stage7 (analytical, no ranking)')
    parser.add_argument('--policy',type=Path,default=None,help='Prespecified JSON scenario; never benchmark-derived')
    parser.add_argument('--output-dir',type=Path,default=None)
    args=parser.parse_args()
    result=generate(ROOT,args.output_dir,args.policy)
    print(f"Stage7: {result['candidate_count']} candidate cost proxies; no measured latency, ranking or HBench.")
    print('See outputs/tpcx_iot_stage7/45_candidate_cost_matrix.csv')

if __name__=='__main__':main()
