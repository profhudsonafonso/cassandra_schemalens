"""Command-line tool for pre-benchmark Stage 8 consolidation."""
import argparse
from pathlib import Path
from methodology.tpcx_iot.stage4.candidates import ROOT
from .consolidator import generate

def main():
    ap=argparse.ArgumentParser(description='TPCx-IoT / SchemaLens Cassandra Stage8: conditional HBench')
    ap.add_argument('--policy',type=Path,default=None)
    ap.add_argument('--output-dir',type=Path,default=None)
    args=ap.parse_args()
    result=generate(ROOT,args.output_dir,args.policy)
    print('Stage8: {unique_canonical_candidate_count} canonical candidates; {conditional_complete_covers} conditionally complete covers; {pareto_covers} Pareto covers; {hbench_count} HBench configurations.'.format(**result))
    print('No Cassandra execution, no measured performance, no official kit verification.')
    print('See outputs/tpcx_iot_stage8/56_hbench_summary.csv')

if __name__=='__main__':main()
