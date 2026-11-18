"""Human-readable explanation of Stage7 outputs. No live database required."""
from pathlib import Path
from methodology.tpcx_iot.stage4.candidates import ROOT, read_json
from methodology.tpcx_iot.stage7.estimator import generate

def main():
    generate(ROOT)
    output=ROOT/'outputs/tpcx_iot_stage7'
    rows={x['candidate_id']:x for x in read_json(output/'41_physical_footprint_proxies.json')}
    reads={x['candidate_id']:x for x in read_json(output/'42_read_cost_proxies.json')}
    issues={x['candidate_id']:x for x in read_json(output/'44_feasibility_diagnostics.json')}
    print('CANDIDATE                  PARTITIONS   MAX ROWS/PARTITION   CQL REQUESTS   WARNING CODES')
    for cid in sorted(rows):
        f=rows[cid];r=reads[cid];w=issues[cid]
        print(f'{cid:27} {f["estimated_physical_partitions"]:10d} '
              f'{f["max_rows_per_partition_proxy"]:20d} '
              f'{r["total_cql_requests_per_logical_read"]:14d} '
              f'{", ".join(w["warnings"])}')
    print('\nThese are scenario-dependent structural estimates, not Cassandra measurements.')

if __name__=='__main__':main()
