"""Bind Stage5 physical CQL read requests and merge results.

There is no Cassandra client in this module. The binder emits CQL requests that
can later be executed by a Cassandra driver; offline.py uses an in-memory index.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass(frozen=True)
class ClosedWindow:
    start_ms: int
    end_ms: int

    def __post_init__(self):
        if any(type(x) is not int for x in (self.start_ms,self.end_ms)):
            raise ValueError('Window bounds must be integer epoch milliseconds')
        if self.end_ms-self.start_ms!=5000:
            raise ValueError('TPCx-IoT research profile needs exactly five seconds per window')


def bucket_starts(start_ms: int, end_ms: int, bucket_seconds: int) -> list[int]:
    """Enumerate all intersected CLOSED UTC buckets, including boundary bucket."""
    if type(bucket_seconds) is not int or bucket_seconds < 1:raise ValueError('Invalid bucket width')
    if type(start_ms) is not int or type(end_ms) is not int or end_ms < start_ms:
        raise ValueError('Invalid time bounds')
    period=bucket_seconds*1000
    first=(start_ms//period)*period
    last=(end_ms//period)*period
    if (last-first)//period>100000: raise ValueError('Unbounded bucket traversal')
    return list(range(first,last+period,period))


def normalize_station_roster(stations: list[str]|None, complete: bool,
                             provenance: str|None) -> list[str]:
    if complete is not True or not provenance or not provenance.strip():
        raise ValueError('An externally verified, provenance-labeled complete station roster is mandatory')
    if not isinstance(stations,list) or not stations:
        raise ValueError('Missing complete station roster')
    if any(not isinstance(v,str) or not v.strip() for v in stations) or len(set(stations))!=len(stations):
        raise ValueError('Invalid or duplicate station identifier')
    return sorted(stations)


def bind_read_requests(plan: dict[str,Any], *, sensor_name: str,
                       recent: ClosedWindow, historical: ClosedWindow,
                       stations: list[str]|None=None, complete_station_roster: bool=False,
                       roster_provenance: str|None=None) -> list[dict[str,Any]]:
    """Emit CQL requests with correctly ordered binds; no filtering or time OR.

    The complete-roster flag is a caller assertion, NOT independent evidence.
    Stage6 must maintain/validate this roster and all TS3 projection copies.
    """
    if not isinstance(sensor_name,str) or not sensor_name.strip():raise ValueError('Sensor name required')
    if not isinstance(recent,ClosedWindow) or not isinstance(historical,ClosedWindow):
        raise ValueError('Two closed 5s windows required')
    if not (historical.end_ms<=recent.start_ms and
            historical.start_ms>=recent.start_ms-1_800_000):
        raise ValueError('Historic interval must be within the previous 1800s of TI1 start')
    if plan.get('query_id')!='Q_READ_2_WINDOWS' or plan.get('read_coverage_status')!='CONDITIONAL_OFFLINE_ONLY':
        raise ValueError('Unexpected or unvalidated QueryPlan')
    if plan['interval_bounds']!='inclusive' or plan['logical_time_ranges']!=2:
        raise ValueError('QueryPlan temporal contract changed')
    if 'ALLOW FILTERING' in plan['read_cql'].upper() or ' OR ' in plan['read_cql'].upper():
        raise ValueError('Unsupported CQL access')
    mode=plan['access_mode']
    if mode=='station_and_sensor':
        stations=normalize_station_roster(stations,complete_station_roster,roster_provenance)
    elif mode=='sensor_only':
        stations=[None]
    else:raise ValueError('Unknown query access mode')
    bucket=plan['time_bucket_seconds']
    requests=[]
    for wid,window in (('TI1_RECENT',recent),('TI2_HISTORICAL',historical)):
        for station in stations:
            buckets=bucket_starts(window.start_ms,window.end_ms,bucket) if bucket else [None]
            for start in buckets:
                if mode=='station_and_sensor':
                    params=[station,sensor_name]
                else:
                    params=[sensor_name]
                if start is not None:params.append(start)
                params.extend([window.start_ms,window.end_ms])
                if plan['read_cql'].count('?')!=len(params):
                    raise ValueError('CQL placeholder/bind mismatch')
                requests.append({
                    'plan_id':plan['plan_id'],'candidate_id':plan['candidate_id'],
                    'window_id':wid,'station_id':station,'sensor_name':sensor_name,
                    'bucket_start_ms':start,'range_start_ms':window.start_ms,
                    'range_end_ms':window.end_ms,'cql':plan['read_cql'],
                    'params':params,
                    'roster_assertion_source':roster_provenance if mode=='station_and_sensor' else None,
                })
    return requests


def merge_read_results(plan: dict[str,Any], batches: list[tuple[dict[str,Any],list[dict[str,Any]]]],
                       *, sensor_name: str,
                       recent: ClosedWindow, historical: ClosedWindow) -> list[dict[str,Any]]:
    """SQL OR / bag result: dedupe only duplicate PHYSICAL rows across reads.

    DISTINCT on (timestamp,reading) would incorrectly discard rows from
    different stations or event IDs with the same measurements.
    """
    identities={}
    allowed_windows={'TI1_RECENT':recent,'TI2_HISTORICAL':historical}
    for req,rows in batches:
        if req.get('plan_id')!=plan['plan_id'] or req.get('window_id') not in allowed_windows:
            raise ValueError('Unknown request or window response')
        expected=allowed_windows[req['window_id']]
        for r in rows:
            for key in ('driver_system_id','sensor_name','event_ts','event_id','sensor_reading'):
                if key not in r:raise ValueError('Incomplete response row')
            if r['sensor_name']!=sensor_name:
                raise ValueError('Incorrect sensor returned')
            if req['station_id'] is not None and r['driver_system_id']!=req['station_id']:
                raise ValueError('Row outside requested station')
            ts=r['event_ts']
            if type(ts) is not int or not expected.start_ms<=ts<=expected.end_ms:
                raise ValueError('Row outside queried time range')
            if plan['time_bucket_seconds'] is not None:
                if (ts//(plan['time_bucket_seconds']*1000))*(plan['time_bucket_seconds']*1000)!=req['bucket_start_ms']:
                    raise ValueError('Row from wrong bucket')
            key=tuple(r[k] for k in plan['merge_identity'])
            if key in identities and identities[key]!=r:
                raise ValueError('Conflicting physical event contents during merge')
            identities[key]=r
    # SQL query does not specify ORDER BY; stable order is only for test reproducibility.
    sorted_keys=sorted(identities,key=lambda k:(k[2],k[0],k[1],str(k[3])))
    return [{'event_ts':identities[k]['event_ts'],'sensor_reading':identities[k]['sensor_reading']}
            for k in sorted_keys]
