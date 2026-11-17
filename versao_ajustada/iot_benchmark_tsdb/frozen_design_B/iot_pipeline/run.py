"""Full offline pipeline. Does NOT connect to Cassandra, ever."""
from __future__ import annotations
import argparse
import subprocess
import sys
from .shared import ROOT, A1,inputs
from . import stage4,stage5,stage6,stage7,stage8

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--regenerate-a1',action='store_true',help='Regenerate illustrative A1 inputs from bundled fixture')
    args=ap.parse_args()
    if args.regenerate_a1:
        subprocess.run([sys.executable,str(A1/'tools/run_a1.py')],cwd=A1,check=True)
    inputs()
    for label,fn in [('4 Candidates',stage4.run),('5 QueryPlans',stage5.run),('6 WritePlans',stage6.run),
                     ('7 Analytical proxies',stage7.run),('8 Freeze HBench',stage8.run)]:
        r=fn();print(label,'OK',len(r))
    print('STOP BEFORE BENCHMARK: Stage8 HBench frozen. No Cassandra calls performed.')
if __name__=='__main__':main()
