"""Read-only plan-aware Rev3 dispatcher for an ALREADY materialized Rev2 table.

The same physical schema can have multiple executable plans. A frozen plan_id
is mandatory; dispatch never guesses an execution strategy from a family alone.
"""
from __future__ import annotations
from collections import Counter
import re
from .core import PLAN_MODES, stable_id

TS0_SELECT = ('SELECT timestamp_ms,value_double FROM ts0_iot_raw '
              'WHERE device_id=%s AND sensor_name=%s')
TS0_LATEST = TS0_SELECT + ' LIMIT 1'

class TracedSession:
    """Record Python execute calls / consumed rows; not server-side read counts."""
    def __init__(self, session):
        self.session=session
        self.execute_calls=0
        self.iterated_rows=0
        self.sql_counts=Counter()
    def execute(self,statement,params=None,*args,**kwargs):
        if not isinstance(statement,str) or not statement.startswith('SELECT '):
            raise ValueError('Read-only executor refused non-SELECT CQL')
        if not re.fullmatch(r'SELECT timestamp_ms,value_double FROM ts0_iot_raw WHERE '
              r'device_id=%s AND sensor_name=%s(?: AND timestamp_ms >= %s '
              r'AND timestamp_ms < %s| LIMIT 1)?',statement):
            raise ValueError('Unexpected TS0 SQL in frozen plan')
        if args or kwargs or params is None:raise ValueError('Unexpected CQL parameters')
        self.execute_calls+=1
        self.sql_counts['LIMIT1' if statement==TS0_LATEST else 'SCAN']+=1
        result=self.session.execute(statement,params)
        owner=self
        class Iter:
            def __iter__(self):
                for r in result:
                    owner.iterated_rows+=1
                    yield r
        return Iter()


def read_ts0_latest(session,query,oracle):
    """DESC+LIMIT1 is safe for TS0 non-null stored cells.

    An unexpected null-valued latest row triggers a conservative FULL SCAN of
    that sensor partition, preserving result semantics rather than hiding data.
    This path is recorded and should not appear in controlled E5-style runs.
    """
    if query['query_id']!='Q8':raise ValueError('TS0 DESC_LIMIT1 only valid for Q8')
    if query.get('value_lower') is not None:raise ValueError('Extra value predicate not supported')
    records=[];fallback=0
    for device in query['devices']:
        for sensor in query['sensors']:
            first=next(iter(session.execute(TS0_LATEST,(device,sensor))),None)
            if first is None:continue
            if first.value_double is None:
                fallback+=1
                for row in session.execute(TS0_SELECT,(device,sensor)):
                    if row.value_double is not None:
                        records.append({'device_id':device,'sensor_name':sensor,
                            'timestamp':int(row.timestamp_ms),'value_double':float(row.value_double)})
            else:
                records.append({'device_id':device,'sensor_name':sensor,
                    'timestamp':int(first.timestamp_ms),'value_double':float(first.value_double)})
    return oracle(records,query),fallback


def verify_frozen_route(design,query,catalog):
    qid=query['query_id']
    route=design['query_routes'][qid]
    cid=route['candidate_id'];mode=route.get('physical_plan')
    if mode not in PLAN_MODES:raise ValueError('Unknown physical plan mode')
    pid=stable_id(qid,cid,mode)
    if pid!=route.get('plan_id'):
        raise ValueError('Executor refused plan_id mismatch with the frozen HBench')
    rec=catalog.get(pid)
    if not rec or rec['candidate_id']!=cid or rec['physical_plan']!=mode or rec['route_status']!='DIRECT':
        raise ValueError('Frozen plan ID not found in executable Stage5 catalog')
    if cid not in design['candidates']:
        raise ValueError('Frozen design lacks the table used by query plan')
    return route


def dispatch(session,query,design,catalog,sensors,days,page_size,oracle):
    """Run EXACTLY the selected plan, returning result and client instrumentation.

    Existing Rev2 keyspaces are used only by the separate probe tool. There is
    no DDL, DML or keyspace reset in this module.
    """
    route=verify_frozen_route(design,query,catalog)
    mode=route['physical_plan'];cid=route['candidate_id']
    if mode.startswith('TS0_'):
        traced=TracedSession(session)
        if mode=='TS0_DESC_LIMIT1':
            result,fallback=read_ts0_latest(traced,query,oracle)
        elif mode in ('TS0_FULL_SCAN','TS0_RANGE_SCAN'):
            from iot_pipeline.stage9 import execute_query
            result=execute_query(traced,query,route,sensors)
            fallback=0
        else:raise ValueError('Invalid TS0 plan')
        return result,{'execute_calls':traced.execute_calls,
          'iterated_rows':traced.iterated_rows,'fallbacks':fallback,'physical_plan':mode}
    if mode.startswith(('TS2U_','TS2D_')):
        from v4_rev2.runtime_helpers import read_wide
        bucket=mode.startswith('TS2D_')
        if cid!=('ts2_iot_wide' if bucket else 'ts2_iot_wide_unbucketed'):
            raise ValueError('Physical TS2 table differs from frozen plan')
        records,calls,rows=read_wide(session,query,sensors,cid,days,bucket=bucket,
             optimized_q8=mode.endswith('_DESC_SPARSE'),page_size=page_size)
        return oracle(records,query),{'execute_calls':calls,'iterated_rows':rows,
                          'fallbacks':0,'physical_plan':mode}
    if mode=='TS4_SNAPSHOT_LOOKUP':
        if query['query_id']!='Q8' or cid!='ts4_iot_latest':raise ValueError('Invalid TS4 route')
        records=[];calls=0;rows=0
        for d in query['devices']:
            for s in query['sensors']:
                calls+=1
                for r in session.execute('SELECT latest_timestamp_ms,value_double FROM ts4_iot_latest '
                                         'WHERE device_id=%s AND sensor_name=%s',(d,s)):
                    rows+=1
                    if r.latest_timestamp_ms is not None and r.value_double is not None:
                        records.append({'device_id':d,'sensor_name':s,
                            'timestamp':int(r.latest_timestamp_ms),'value_double':float(r.value_double)})
        return oracle(records,query),{'execute_calls':calls,'iterated_rows':rows,
                                'fallbacks':0,'physical_plan':mode}
    raise ValueError('Unimplemented physical plan ID')
