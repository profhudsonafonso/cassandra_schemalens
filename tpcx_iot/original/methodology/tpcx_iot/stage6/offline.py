"""Deterministic correctness probes; no Cassandra cluster or performance timers."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from typing import Any
from uuid import UUID

from methodology.tpcx_iot.stage5.offline import fixtures, logical_oracle
from methodology.tpcx_iot.stage5.runtime import ClosedWindow
from .materializer import (
    AuthoritativeRoster, InMemoryMutationSink, PartialWriteFailure,
    ReferenceMaterializer, event_uuid, json_ready,
)


def bag(items:list[dict])->Counter:
    return Counter((r['event_ts'],r['sensor_reading']) for r in items)


def check_single(plans:list[dict],reads:dict,fixture:tuple)->list[dict]:
    name,recent,historical,rows=fixture
    stations=sorted({e['driver_system_id'] for e in rows}) or ['A']
    roster=AuthoritativeRoster(tuple(stations),'offline_fixture_authoritative_roster',True)
    checks=[]
    for plan in plans:
        materializer=ReferenceMaterializer(plans,roster,[plan['candidate_id']])
        normalized=[]
        for i,event in enumerate(rows):
            d=dict(event)
            d['event_id']=UUID(d['event_id'])
            normalized.append(d)
        first=materializer.apply(normalized)
        duplicate=materializer.apply(normalized)
        answer,requests=materializer.mock_read(reads[plan['candidate_id']],sensor_name='temp',
                                                recent=recent,historical=historical)
        expected=logical_oracle(rows,sensor_name='temp',recent=recent,historical=historical)
        equal=bag(answer)==bag(expected)
        valid=(equal and len(first)==len(rows) and len(duplicate)==0 and
               len(materializer.rows[plan['candidate_id']])==len(rows))
        checks.append(dict(fixture=name,candidate_id=plan['candidate_id'],family=plan['family'],
                           events=len(rows),first_insert_mutations=len(first),
                           duplicate_replay_mutations=len(duplicate),query_requests=requests,
                           expected_result_count=len(expected),actual_result_count=len(answer),passed=valid,
                           scope='OFFLINE_REFERENCE_ONLY'))
    return checks


def run_offline_suite(plans:list[Any],reads:list[dict])->tuple[list[dict],list[dict]]:
    ps=[asdict(p) if not isinstance(p,dict) else p for p in plans]
    rp={p['candidate_id']:p for p in reads}
    probes=[rec for fixture in fixtures() for rec in check_single(ps,rp,fixture)]
    if len(probes)!=25:raise ValueError('Expected 25 independent per-candidate write/read fixtures')
    roster=AuthoritativeRoster(('A','B'),'deterministic_fixture_declared_source_catalog',True)
    model=ReferenceMaterializer(ps,roster)
    def e(station,sensor,ts,reading,source_id):
        return dict(driver_system_id=station,sensor_name=sensor,event_ts=ts,
                    sensor_reading=reading,padding='retained_padding',source_record_id=source_id)
    sample=[e('A','temp',59_000,1.5,'e0'), e('B','temp',60_000,1.5,'e1'),
            e('A','temp',60_000,2.0,'e2'), e('A','temp',60_000,2.0,'e3'),
            e('B','temp',64_000,9.0,'e4'),e('A','voltage',100_000,30,'e5')]
    changes=model.apply(sample)
    same=model.apply(sample)
    recovered=model.recovery_replay()
    sink=InMemoryMutationSink()
    failed=False
    try:
        sink.apply(changes,fail_after=3)
    except PartialWriteFailure:
        failed=True
    was_partial=failed and not sink.is_equal_to(model)
    sink.apply(recovered)
    repaired=sink.is_equal_to(model)
    recent=ClosedWindow(59_000,64_000)
    historical=ClosedWindow(0,5_000)
    readings={}
    for p in ps:
        res,num=model.mock_read(rp[p['candidate_id']],sensor_name='temp',
                                recent=recent,historical=historical)
        readings[p['candidate_id']]=(bag(res),num)
    uniform_read_bags=len({tuple(sorted(v[0].items())) for v in readings.values()})==1
    first=sample[0]
    expected_uuid=event_uuid(first['driver_system_id'],first['sensor_name'],first['event_ts'],first['source_record_id'])
    identifier_ok=all(a['parameters'].count(expected_uuid)==1 for a in changes if a['row']['event_id']==expected_uuid)
    results=[
        ('multi_table_fanout',len(changes)==len(sample)*len(ps)),
        ('same_event_replay_is_noop',not same),
        ('partial_failure_is_observable',was_partial),
        ('recovery_replay_converges',repaired),
        ('same_timestamp_distinct_ids_preserved',len(model.events)==len(sample)),
        ('source_id_deterministic_uuid',identifier_ok),
        ('read_after_write_equal_across_five_candidates',uniform_read_bags),
    ]
    probes += [dict(fixture=name,candidate_id='ALL',family='TS0/TS1/TS3',passed=okay,
                    scope='OFFLINE_REFERENCE_ONLY') for name,okay in results]
    return probes,json_ready(changes[:min(10,len(changes))])
