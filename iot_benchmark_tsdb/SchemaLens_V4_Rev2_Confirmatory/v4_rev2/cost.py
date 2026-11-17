"""Rev2 physical-plan proxy (offline). No measured performance imports.

Two distinct cost drivers are intentionally represented:
 - request_count: CQL request/driver round trip
 - bucket_fanout: additional cross-partition grouping/coordination above one access
The coefficients are transparent policy assumptions, not calibrated to V3/V4 latencies.
"""
from __future__ import annotations
from collections import defaultdict
import hashlib
from pathlib import Path
import json
import math
from v4_auto import selector as A

MILLIS_DAY=86_400_000
REVISION='V4_AUTO_CANONICAL_CROSS_QUERY_REV2'
REV2_POLICY={
 'request_cost_units':4.0, 'physical_row_cost_units':0.02,
 'returned_kib_cost_units':0.20, 'extra_bucket_coordination_units':2.0,
 'partition_soft_limit_bytes':32*1024*1024,
 'partition_hard_limit_bytes':128*1024*1024,
 'partition_pressure_cost_units':5.0,
 'row_header_bytes_proxy':24,
 'metric_double_bytes_proxy':8,
 'ts0_cell_bytes_proxy':48,
 'notes':{
   'provenance':'V4 Rev2 prospectively declared engineering assumptions, NOT fitted to observed p95',
   'requests':'CQL round trip estimated from schema key constraints',
   'extra_bucket_coordination':'Independent fan-out merge/group cost: max(0, bucket_scans - logical operands)',
   'partition_pressure':'Only applies above 32MiB estimated partition size; no price advantage granted arbitrarily at tiny scale',
   'bytes':'Simplified payload/partition size proxy, NOT real on-disk bytes',
   'policy':'Uniform Q1-Q12 weights because native scheduler frequencies were not captured'
 }
}

def day_bucket(start,end,known_days):
    """Match executor day_indices: scan ALL requested buckets, even empty ones.

    For unbounded Q5 the executor enumerates known days. For bounded query
    ranges it loops over every calendar bucket touched by [start,end).
    """
    if start is None:return sorted(known_days)
    count=(end-1)//MILLIS_DAY-start//MILLIS_DAY+1
    if count<1 or count>36600:raise ValueError("Temporal bucket span unsupported by Rev2 guard")
    return list(range(start//MILLIS_DAY,(end-1)//MILLIS_DAY+1))

def trace(q,cid,rows,requests_for,physical_metadata,policy):
    fam='TS0' if cid=='ts0_iot_raw' else 'TS4' if cid==A.LATEST else 'TS2'
    bucket=cid=='ts2_iot_wide'; devices=defaultdict(lambda:defaultdict(set))
    for r in rows:devices[r['device_id']][r['timestamp']].add(r['sensor_name'])
    maxdays={d:{t//MILLIS_DAY for t in db} for d,db in devices.items()}
    scans=0;phys_rows=0;bucket_count=0;operands=0
    if fam=='TS4':
        operands=len(q['devices'])*len(q['sensors']);scans=operands;phys_rows=operands
    elif q['query_id']=='Q8':
        for device in q['devices']:
            operands+=1
            if fam=='TS0':
                scans+=len(q['sensors']);phys_rows+=len(q['sensors'])
            else:
                requested=set(q['sensors']);seen=[]
                for t in sorted(devices[device],reverse=True):
                    seen.append(t)
                    requested-=devices[device][t]
                    if not requested:break
                phys_rows+=len(seen)
                n= len({t//MILLIS_DAY for t in seen}) if bucket else 1
                scans+=n;bucket_count+=n if bucket else 0
    else:
        for device,sensors,start,end in requests_for(q):
            operands+=1
            if fam=='TS0':
                scans+=len(sensors)
                phys_rows+=sum(len(v.intersection(sensors)) for t,v in devices[device].items()
                               if start is None or start<=t<end)
            else:
                days=day_bucket(start,end,maxdays.get(device,set())) if bucket else [None]
                scans+=len(days)
                if bucket:bucket_count+=len(days)
                phys_rows+=sum(1 for t in devices[device] if start is None or start<=t<end)
    sensors_count=len(set(q['sensors']))
    payload_per_row=policy['ts0_cell_bytes_proxy'] if fam=='TS0' else (
        32 if fam=='TS4' else policy['row_header_bytes_proxy'] + policy['metric_double_bytes_proxy']*sensors_count)
    payload=phys_rows*payload_per_row
    # The fan-out term measures additional merge/coordination overhead *beyond*
    # the base logical operand accesses already captured by 'requests'.
    fanout=max(0,bucket_count-operands)
    partition_bytes=physical_metadata.get(cid,{}).get('max_partition_bytes_proxy',0)
    risk=max(0,partition_bytes/policy['partition_soft_limit_bytes']-1)
    return {'request_count_proxy':scans,'physical_rows_proxy':phys_rows,
            'bucket_count_proxy':bucket_count,'logical_operand_count':operands,
            'extra_bucket_fanout_proxy':fanout,'returned_bytes_proxy':payload,
            'max_partition_bytes_proxy':partition_bytes,'partition_pressure_proxy':round(risk,8)}

def physical_partitions(rows,eligible,policy):
    partitions={}
    for cand in eligible:
        cid=cand['candidate_id'];m=defaultdict(int)
        for row in rows:
            d,t,s=row['device_id'],row['timestamp'],row['sensor_name']
            if cid=='ts0_iot_raw': key=(d,s)
            elif cid==A.LATEST:key=(d,s)
            elif cid=='ts2_iot_wide':key=(d,t//MILLIS_DAY)
            else:key=(d,)
            m[key]+=1
        # Wide family has multiple sensor cells in one row; account for distinct timestamp rows.
        if cid.startswith('ts2'):
            unique=defaultdict(set)
            for row in rows:
                d,t=row['device_id'],row['timestamp']
                key=(d,t//MILLIS_DAY) if cid=='ts2_iot_wide' else (d,)
                unique[key].add(t)
            m={k:len(x) for k,x in unique.items()}
        elif cid==A.LATEST:m={k:1 for k in m}
        row_bytes=(policy['row_header_bytes_proxy'] + policy['metric_double_bytes_proxy'] * len({r['sensor_name'] for r in rows})) if cid.startswith('ts2') else policy['ts0_cell_bytes_proxy']
        peak=max(m.values(),default=0)
        peak_bytes=peak*row_bytes
        partitions[cid]={'candidate_id':cid,'estimated_partition_count':len(m),'max_partition_rows_proxy':peak,
             'max_partition_bytes_proxy':peak_bytes,'row_size_bytes_proxy':row_bytes,
             'partition_soft_limit_bytes':policy['partition_soft_limit_bytes'],
             'partition_hard_limit_bytes':policy['partition_hard_limit_bytes'],
             'hard_guard_pass':peak_bytes<=policy['partition_hard_limit_bytes']}
    return partitions

def derive_rev2(rows,queries,eligible,plans,original_policy,requests_for,rev2_policy=None):
    p=dict(REV2_POLICY if rev2_policy is None else rev2_policy)
    # Reuse Rev1 Stage6 accounting to avoid accidentally changing physical candidate semantics.
    writes,storage,_=A.derive_analytic(rows,queries,eligible,plans,original_policy,requests_for)
    partition_meta=physical_partitions(rows,eligible,p)
    qs={q['query_id']:q for q in queries}
    read=[]
    for plan in plans:
        if plan['route_status']!='DIRECT':continue
        cid=plan['candidate_id'];q=qs[plan['query_id']]
        details=trace(q,cid,rows,requests_for,partition_meta,p)
        proxy=(p['request_cost_units']*details['request_count_proxy']+
               p['physical_row_cost_units']*details['physical_rows_proxy']+
               p['returned_kib_cost_units']*details['returned_bytes_proxy']/1024+
               p['extra_bucket_coordination_units']*details['extra_bucket_fanout_proxy']+
               p['partition_pressure_cost_units']*details['partition_pressure_proxy'])
        read.append({'plan_id':plan['plan_id'],'query_id':q['query_id'],'candidate_id':cid,
            'analytic_read_proxy':round(proxy,8),**details,
            'coefficient_policy':'V4_REV2_PREDECLARED_NO_MEASURED_PERFORMANCE',
            'estimation_only':True})
    return writes,storage,read,partition_meta,p

def select_rev2(eligible,plans,storage,writes,reads,queries,policy):
    candidates=eligible
    # Reuse transparent Pareto + structural-diversity logic from the preexisting algorithm.
    allcovers,selected,pareto=A.search_covers(candidates,plans,storage,writes,reads,queries,policy)
    for record in allcovers:
        # A new methodological revision must have new, immutable deployment IDs.
        stamp=REVISION+'|'+','.join(record['candidates'])+'|'+A.digest_bytes(str(sorted(record['query_routes'].items())).encode('utf-8'))
        record['deployment_id']='R2H_'+hashlib.sha256(stamp.encode('utf-8')).hexdigest()[:12]
        record['selection_policy_revision']=REVISION
        record['analytical_tie_note']='Same estimated features can yield a Pareto tie; do not use measured p95 to break it'
    return allcovers,selected,pareto
