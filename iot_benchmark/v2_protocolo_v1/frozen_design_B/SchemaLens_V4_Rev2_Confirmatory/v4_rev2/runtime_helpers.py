"""Cassandra read paths used by the frozen V4 Rev2 experiment.

Contains NO keyspace creation/deletion; benchmark.py owns that guarded action.
Queries use the same projected metrics for the baseline and TS2 family.
"""
from __future__ import annotations
import math

DAY=86_400_000

def pctl(values,p):
    if not values:raise ValueError('Cannot estimate a percentile from zero runs')
    ordered=sorted(values)
    return ordered[max(0,min(len(ordered)-1,math.ceil(len(ordered)*p/100)-1))]

def read_wide(session,q,sensors,table,days,*,bucket=False,optimized_q8=False,page_size=16):
    """Projected Cassandra reads with correct Q12 child windows and sparse-null Q8.

    Q8 visits descending pages until it has found the latest non-null value
    separately for every projected metric. Cassandra may fetch more rows in
    a page than were iterated; 'iterated_rows' is NOT server scan effort.
    """
    from iot_pipeline.native_reference_v3 import requests_for
    from iot_pipeline.reference_baseline import columns_for
    from iot_pipeline.offline import date_of,day_indices
    if table not in ('iot_reference_table','ts2_iot_wide_unbucketed','ts2_iot_wide'):
        raise ValueError('Unexpected Cassandra table in Rev2 read')
    allowed=set(columns_for(sensors));out={};calls=0;iterated=0
    if q['query_id']=='Q8' and optimized_q8:
        from cassandra.query import SimpleStatement
        for device in q['devices']:
            projected=tuple(q['sensors']);missing=set(projected)
            cols=['m_'+s for s in projected]
            if not set(cols)<=allowed:raise ValueError('Invalid metric projection')
            ordered_days=sorted(days,reverse=True) if bucket else (None,)
            for day in ordered_days:
                cql='SELECT timestamp_ms,'+','.join(cols)+f' FROM {table} WHERE device_id=%s'
                params=(device,)
                if bucket:cql+=' AND bucket_day=%s';params+=(date_of(day*DAY),)
                calls+=1
                result=session.execute(SimpleStatement(cql,fetch_size=page_size),params)
                for row in result:
                    iterated+=1
                    for col,sensor in zip(cols,projected):
                        if sensor not in missing:continue
                        value=getattr(row,col,None)
                        if value is None:continue
                        ts=int(row.timestamp_ms)
                        out[device,sensor,ts]={'device_id':device,'sensor_name':sensor,
                                             'timestamp':ts,'value_double':float(value)}
                        missing.remove(sensor)
                    if not missing:break
                if not missing:break
        return list(out.values()),calls,iterated
    for device,projection,start,end in requests_for(q):
        cols=['m_'+s for s in projection]
        if not set(cols)<=allowed:raise ValueError('Invalid metric projection')
        if bucket:
            daylist=list(day_indices(start,end)) if start is not None else sorted(days)
        else:daylist=[None]
        for day in daylist:
            cql='SELECT timestamp_ms,'+','.join(cols)+f' FROM {table} WHERE device_id=%s'
            params=(device,)
            if bucket:cql+=' AND bucket_day=%s';params+=(date_of(day*DAY),)
            if start is not None:cql+=' AND timestamp_ms >= %s AND timestamp_ms < %s';params+=(start,end)
            calls+=1
            for row in session.execute(cql,params):
                iterated+=1;timestamp=int(row.timestamp_ms)
                for col,sensor in zip(cols,projection):
                    v=getattr(row,col,None)
                    if v is None:continue
                    out[device,sensor,timestamp]={'device_id':device,'sensor_name':sensor,
                                                  'timestamp':timestamp,'value_double':float(v)}
    return list(out.values()),calls,iterated

def load_wide(session,rows,sensors,table,bucket):
    from iot_pipeline.reference_baseline import build_wide_records,columns_for
    from iot_pipeline.offline import date_of
    if table not in ('iot_reference_table','ts2_iot_wide_unbucketed','ts2_iot_wide'):
        raise ValueError('Unexpected physical table in Rev2 loader')
    wide=build_wide_records(rows,sensors)
    cols=['device_id']+(['bucket_day'] if bucket else [])+['timestamp_ms']+columns_for(sensors)
    sql=f'INSERT INTO {table} ({",".join(cols)}) VALUES ({",".join("?" for _ in cols)})'
    prepared=session.prepare(sql)
    for dev,timestamp,*metric_values in wide:
        values=(dev,date_of(timestamp),timestamp,*metric_values) if bucket else (dev,timestamp,*metric_values)
        session.execute(prepared,values)
    return len(wide)
