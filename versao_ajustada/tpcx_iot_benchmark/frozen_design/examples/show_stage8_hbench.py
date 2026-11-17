"""Explain the pre-benchmark *conditional* HBench, not measured winners."""
from methodology.tpcx_iot.stage4.candidates import ROOT,read_json

if __name__=='__main__':
    folder=ROOT/'outputs/tpcx_iot_stage8'
    m=read_json(folder/'58_stage8_manifest.json')
    print(f"{m['active_candidate_count']} unique physical source candidates; {m['conditional_complete_covers']} conditional complete covers; {m['pareto_covers']} Pareto alternatives")
    print('HBench (proxy-only, no Cassandra live runs):')
    for h in read_json(folder/'54_hbench_selected.json'):
        print(f"  {h['hbench_selection_order']} {h['deployment_id']}: {', '.join(h['tables'])}; "
              f"read={h['route']['Q_READ_2_WINDOWS']['candidate_id']}; "
              f"CQL calls={h['objectives']['read_cql_requests']}; "
              f"write mutations/event={h['objectives']['insert_statements_per_event']}")
    print('Stage9 gates: kit predicates, full roster, live read/write coverage, durable repair if TS3.')
