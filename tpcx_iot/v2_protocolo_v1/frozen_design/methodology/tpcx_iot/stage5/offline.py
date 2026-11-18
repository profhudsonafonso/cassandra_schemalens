"""Independent, deterministic logical-oracle probes for Stage5 read plans.

Physical candidate rows are indexed by their actual declared partition keys.
This is not a Cassandra connection, query planner, performance measurement,
or proof of equivalent behavior under real concurrent ingest.
"""
from __future__ import annotations

from collections import Counter
from uuid import uuid5, NAMESPACE_DNS
from typing import Any

from .runtime import ClosedWindow, bind_read_requests, merge_read_results


def event(station: str, sensor: str, ts: int, reading: float, salt: str='0') -> dict[str,Any]:
    return {'driver_system_id':station,'sensor_name':sensor,'event_ts':ts,
            'event_id':str(uuid5(NAMESPACE_DNS,f'{station}/{sensor}/{ts}/{salt}')),
            'sensor_reading':reading,'padding':'payload_'+salt}


def fixtures():
    return [
        ('multiple_stations_and_equal_readings',
         ClosedWindow(200_000,205_000),ClosedWindow(100_000,105_000),[
             event('A','temp',100_000,1),event('B','temp',100_000,1),
             event('A','temp',105_000,2),event('A','temp',200_000,3),
             event('A','temp',205_000,4),event('B','temp',201_000,5),
             event('A','temp',200_000,3,'different_uuid'),
             event('B','voltage',201_000,88),event('A','temp',205_001,10)]),
        ('bucket_boundary_60s',
         ClosedWindow(59_000,64_000),ClosedWindow(0,5_000),[
             event('A','temp',0,0),event('B','temp',5_000,1),
             event('A','temp',59_000,2),event('B','temp',59_999,3),
             event('A','temp',60_000,4),event('A','temp',64_000,5)]),
        ('touching_windows_dedup',
         ClosedWindow(119_000,124_000),ClosedWindow(114_000,119_000),[
             event('A','temp',114_000,1),event('A','temp',119_000,2),
             event('B','temp',119_000,2),event('A','temp',124_000,3)]),
        ('negative_epoch_floor',
         ClosedWindow(-60_500,-55_500),ClosedWindow(-65_500,-60_500),[
             event('A','temp',-65_500,1),event('A','temp',-60_500,2),
             event('B','temp',-60_000,3),event('B','temp',-55_500,4)]),
        ('empty_windows',
         ClosedWindow(160_000,165_000),ClosedWindow(100_000,105_000),[
             event('A','temp',50_000,1),event('B','voltage',162_000,2)]),
    ]


def logical_oracle(events: list[dict[str,Any]], *, sensor_name: str,
                   recent: ClosedWindow,historical: ClosedWindow) -> list[dict[str,Any]]:
    # This function never examines physical layouts, partition keys or bound CQL.
    output=[]
    for r in events:
        t=r['event_ts']
        if r['sensor_name']==sensor_name and (
            recent.start_ms<=t<=recent.end_ms or historical.start_ms<=t<=historical.end_ms
        ):
            output.append({'event_ts':t,'sensor_reading':r['sensor_reading']})
    return output


def physical_index(plan:dict[str,Any],events:list[dict[str,Any]]) -> dict[tuple,list[dict]]:
    idx={}
    for row in events:
        r=dict(row)
        if plan['time_bucket_seconds'] is not None:
            w=plan['time_bucket_seconds']*1000
            r['bucket_start_ms']=(r['event_ts']//w)*w
        key=tuple(r[k] for k in plan['partition_key'])
        idx.setdefault(key,[]).append(r)
    return idx


def mock_cassandra_result(plan:dict[str,Any],req:dict[str,Any],index:dict[tuple,list[dict]]) -> list[dict]:
    """Lookup ONLY keyed partition (no synthetic unrestricted table scan)."""
    all_params=req['params']
    pk_size=len(plan['partition_key'])
    key=tuple(all_params[:pk_size])
    if tuple(req['params'][-2:])!=(req['range_start_ms'],req['range_end_ms']):
        raise ValueError('Mismatched time binds')
    return [r.copy() for r in index.get(key,[]) if req['range_start_ms']<=r['event_ts']<=req['range_end_ms']]


def probe(plan:dict[str,Any],name:str,recent:ClosedWindow,historical:ClosedWindow,
          events:list[dict[str,Any]]) -> dict[str,Any]:
    stations=sorted(set(x['driver_system_id'] for x in events))
    if not stations:stations=['A']
    requests=bind_read_requests(plan,sensor_name='temp',recent=recent,historical=historical,
                                stations=stations,complete_station_roster=True,
                                roster_provenance='synthetic_fixture_exact_station_set')
    idx=physical_index(plan,events)
    batches=[(request,mock_cassandra_result(plan,request,idx)) for request in requests]
    actual=merge_read_results(plan,batches,sensor_name='temp',recent=recent,historical=historical)
    expected=logical_oracle(events,sensor_name='temp',recent=recent,historical=historical)
    def bag(rows:list[dict]):return Counter((x['event_ts'],x['sensor_reading']) for x in rows)
    same=bag(actual)==bag(expected)
    return {'fixture':name,'candidate_id':plan['candidate_id'],'family':plan['family'],
            'cql_request_count':len(requests),'actual_result_count':len(actual),
            'logical_oracle_result_count':len(expected),'passed':same,
            'validation_scope':'DETERMINISTIC_OFFLINE_FIXTURE_NOT_LIVE_CASSANDRA'}


def run_offline_suite(plans:list[Any]) -> list[dict[str,Any]]:
    return [probe(p if isinstance(p,dict) else __import__('dataclasses').asdict(p),name,recent,historical,rows)
            for p in plans for name,recent,historical,rows in fixtures()]
