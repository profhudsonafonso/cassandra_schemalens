"""CLI: python3 -m methodology.tpcx_iot.stage4.cli"""
import argparse
import json
from pathlib import Path
from .candidates import ROOT,instantiate


def main():
    parser=argparse.ArgumentParser(description='SchemaLens TPCx-IoT Cassandra Stage 4 physical candidates')
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--physical-policy',type=Path)
    parser.add_argument('--frozen-dir',type=Path)
    args=parser.parse_args()
    result=instantiate(args.root,args.output_dir,args.physical_policy,args.frozen_dir)
    print(json.dumps({'candidate_count':result['candidate_count'],
                      'counts':result['candidate_counts_by_family'],
                      'benchmark_executed':result['benchmark_executed']}, indent=2))

if __name__=='__main__':main()
