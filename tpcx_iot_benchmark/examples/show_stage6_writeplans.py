"""A pedagogical Stage 6 demo. Run: python3 -m examples.show_stage6_writeplans.

English comments are deliberate: they can be used in supervisor demos.
No Apache Cassandra instance is opened by this program.
"""
from __future__ import annotations

import json

from methodology.tpcx_iot.stage4.candidates import ROOT,read_json
from methodology.tpcx_iot.stage5.runtime import ClosedWindow
from methodology.tpcx_iot.stage6.planner import generate
from methodology.tpcx_iot.stage6.materializer import (
    AuthoritativeRoster,ReferenceMaterializer,InMemoryMutationSink,
    PartialWriteFailure,json_ready,
)


def main():
    generate(ROOT)  # Ensure the outputs exist, without rerunning earlier stages.
    out=ROOT/'outputs/tpcx_iot_stage6'
    write_plans=read_json(out/'30_writeplans.json')
    read_plans=read_json(ROOT/'outputs/tpcx_iot_stage5/20_queryplans.json')

    # This is a caller-asserted COMPLETE catalog, not inferred from observed rows.
    roster=AuthoritativeRoster(('ST01','ST02'),'fixture_authoritative_station_list',True)

    # All five variants are materialized here solely as a correctness test.
    # Stage 8 will decide which physically compatible variants should coexist.
    reference=ReferenceMaterializer(write_plans,roster)

    # source_record_id MUST come from a unique, replay-stable ingestion offset/id.
    # It is not safe to construct this identity from timestamp alone.
    observations=[
        {'driver_system_id':'ST01','sensor_name':'temperature',
         'event_ts':59_000,'sensor_reading':23.1,'padding':'A'*8,'source_record_id':'offset-001'},
        {'driver_system_id':'ST02','sensor_name':'temperature',
         'event_ts':60_000,'sensor_reading':24.0,'padding':'B'*8,'source_record_id':'offset-002'},
        {'driver_system_id':'ST01','sensor_name':'temperature',
         'event_ts':60_000,'sensor_reading':24.0,'padding':'C'*8,'source_record_id':'offset-003'},
        {'driver_system_id':'ST02','sensor_name':'voltage',
         'event_ts':61_000,'sensor_reading':220.0,'padding':'D'*8,'source_record_id':'offset-004'},
    ]
    changes=reference.apply(observations)
    print('1) WritePlans:',len(write_plans))
    for plan in write_plans:
        print(f"  {plan['candidate_id']}: {plan['write_cql']}")
    print('2) Input observations:',len(observations))
    print('3) Physical mutations when ALL variants are materialized:',len(changes))
    print('4) Example TS3 mutation:')
    ts3=next(m for m in changes if m['family']=='TS3')
    print(json.dumps(json_ready({'cql':ts3['cql'],'params':ts3['parameters']}),indent=2))

    # Simulate a failure after THREE mutation acknowledgements.
    simulated=InMemoryMutationSink()
    try:
        simulated.apply(changes,fail_after=3)
    except PartialWriteFailure as err:
        print('5) Expected partial failure:',str(err))
    print('   Fully materialized?',simulated.is_equal_to(reference))

    # Replay deterministic full mutations from our IN-MEMORY source ledger.
    # In production, the source log/outbox must be durable and replayable.
    simulated.apply(reference.recovery_replay())
    print('6) Converged after replay?',simulated.is_equal_to(reference))
    print('7) Repeated input generates new mutations:',len(reference.apply(observations)))

    # Reuse Stage 5 read paths against the actual Stage 6 physical row maps.
    recent=ClosedWindow(59_000,64_000)
    historical=ClosedWindow(0,5_000)
    print('8) Read-after-write check for every candidate:')
    for p in read_plans:
        rows,n=reference.mock_read(p,sensor_name='temperature',recent=recent,historical=historical)
        print(f"  {p['candidate_id']:<21} queries={n}, readings={len(rows)}, values={[r['sensor_reading'] for r in rows]}")
    print('9) LIVE CASSANDRA: NOT EXECUTED. DISTRIBUTED ATOMICITY: NOT PROVEN.')


if __name__=='__main__':main()
