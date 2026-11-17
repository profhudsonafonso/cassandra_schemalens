"""Stage 5-6 reference materializer for correctness checks; NO latency measurements."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timezone
import math
from .shared import *

DAY=86_400_000

def date_of(ms):return datetime.fromtimestamp(ms/1000,timezone.utc).date()
def day_indices(start,end):
    if end<=start: return []
    return range(start//DAY,(end-1)//DAY+1)

def physical_state(rows, window_ms):
    """Construct immutable materializations from canonical logical points."""
    raw={(r['device_id'],r['sensor_name'],r['timestamp']):r['value_double'] for r in rows}
    bucket=defaultdict(dict);wide=defaultdict(dict);wide_unbucketed=defaultdict(dict);latest={};roll=defaultdict(list)
    for r in rows:
        d,s,t,v=r['device_id'],r['sensor_name'],r['timestamp'],r['value_double']
        bucket[(d,s,t//DAY)][t]=v
        wide[(d,t//DAY,t)][s]=v
        wide_unbucketed[(d,t)][s]=v
        if (d,s) not in latest or t>latest[(d,s)][0]:latest[(d,s)]=(t,v)
        roll[(d,s,(t//window_ms)*window_ms)].append(v)
    agg={k:{'sum':sum(vs),'count':len(vs),'min':min(vs),'max':max(vs)} for k,vs in roll.items()}
    return {'TS0':raw,'TS1':bucket,'TS2':wide,'TS2_UNBUCKETED':wide_unbucketed,'TS4':latest,'TS5':agg}

def read_records(state, family,q,candidate_id=None):
    """Return physical rows for a query; logical postprocessing happens only afterward."""
    devices=set(q['devices']);sensors=set(q['sensors']);start,end=q['start_ms'],q['end_ms_exclusive']
    if q['query_id'] in {'Q5','Q8'}:start=-2**63;end=2**63-1
    if family=='TS4':
        result=[]
        for d in sorted(devices):
            for s in sorted(sensors):
                v=state['TS4'].get((d,s))
                if v and start<=v[0]<end:result.append({'device_id':d,'sensor_name':s,'timestamp':v[0],'value_double':v[1]})

        return result
    result=[]
    if family=='TS0':
        for (d,s,t),v in state['TS0'].items():
            if d in devices and s in sensors and start<=t<end:
                result.append({'device_id':d,'sensor_name':s,'timestamp':t,'value_double':v})
    if family=='TS1':
        for (d,s,day),points in state['TS1'].items():
            if d in devices and s in sensors:
                for t,v in points.items():
                    if start<=t<end:result.append({'device_id':d,'sensor_name':s,'timestamp':t,'value_double':v})
    if family=='TS2':
        if candidate_id=='ts2_iot_wide_unbucketed':
            iterator=((d,t,measures) for (d,t),measures in state['TS2_UNBUCKETED'].items())
        else:
            iterator=((d,t,measures) for (d,day,t),measures in state['TS2'].items())
        for d,t,points in iterator:
            if d in devices and start<=t<end:
                for s,v in points.items():
                    if s in sensors:result.append({'device_id':d,'sensor_name':s,'timestamp':t,'value_double':v})
    return result

def from_rollup(state,q):
    """Read aligned rollup; callers must verify both window boundaries."""
    g=q['group_by_ms'];start=q['start_ms'];end=q['end_ms_exclusive']
    if not g or start%g or end%g:raise ValueError('Unsafe TS5 route: unaligned time window')
    result=[]
    for (d,s,w),agg in state['TS5'].items():
        if d not in q['devices'] or s not in q['sensors'] or not start<=w<end:continue
        func=q['aggregation'];v={'count':lambda:agg['count'],'sum':lambda:agg['sum'],
            'min':lambda:agg['min'],'max':lambda:agg['max'],
            'avg':lambda:agg['sum']/agg['count']}[func]()
        result.append({'device_id':d,'sensor_name':s,'window_start_ms':w,'aggregate':v})
    return sorted(result,key=lambda x:(x['device_id'],x['sensor_name'], -x['window_start_ms'] if q['query_id']=='Q11' else x['window_start_ms']))

def compare(got,expected,eps=1e-9):
    """Compare structured reference answers, preserving ordering and scalar tolerance."""
    if type(got)!=type(expected):return False
    if isinstance(got,float):return math.isclose(got,expected,rel_tol=eps,abs_tol=eps)
    if isinstance(got,dict):return got.keys()==expected.keys() and all(compare(got[k],expected[k],eps) for k in got)
    if isinstance(got,(list,tuple)):return len(got)==len(expected) and all(compare(a,b,eps) for a,b in zip(got,expected))
    return got==expected

def offline_validate():
    import sys
    sys.path.insert(0,str(A1))
    from core.bridge import _result
    p=require_policy();rows=read_rows(inputs()['native']);qs=read(inputs()['queries']);selected=read(path4(8,'28_hbench_frozen.json'))
    state=physical_state(rows,p['rollup_window_ms']);checks=[]
    for h in selected:
        for q in qs:
            route=h['query_routes'][q['query_id']];f=route['family']; ref=_result(rows,q)
            if f=='TS5':got=from_rollup(state,q)
            else:got=_result(read_records(state,f,q,candidate_id=route['candidate_id']),q)
            passed=compare(got,ref)
            checks.append({'deployment_id':h['deployment_id'],'query_id':q['query_id'],
                           'route':f,'reference_match':passed,'verification':'IN_MEMORY_REFERENCE_ONLY'})
            if not passed:raise AssertionError(f'Read semantic mismatch {h["deployment_id"]}:{q["query_id"]} route {f}')
    write(path4(8,'33_offline_reference_checks.json'),checks)
    return checks
