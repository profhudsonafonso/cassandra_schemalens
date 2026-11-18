"""Validate query JSONL sampled from IoT Benchmark's Java GenerateQueryWorkLoad.

The official Java export is a *parameter-object sample*, not a replay of mixed-workload
scheduling or a proof of SQL equivalence. This module never calls Cassandra.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

OPERATIONS={
    'PRECISE_QUERY':'Q1','RANGE_QUERY':'Q2','VALUE_RANGE_QUERY':'Q3',
    'AGG_RANGE_QUERY':'Q4','AGG_VALUE_QUERY':'Q5','AGG_RANGE_VALUE_QUERY':'Q6',
    'GROUP_BY_QUERY':'Q7','LATEST_POINT_QUERY':'Q8',
    'RANGE_QUERY_ORDER_BY_TIME_DESC':'Q9',
    'VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC':'Q10',
    'GROUP_BY_QUERY_ORDER_BY_TIME_DESC':'Q11','SET_OP_QUERY':'Q12',
}

def validate(path:Path, expected=None):
    """Return records by Q class; errors on missing/malformed native query metadata."""
    byq={}; counts=Counter()
    lines=path.read_text(encoding='utf-8').splitlines()
    if not lines: raise ValueError('Query capture file is empty')
    for n,line in enumerate(lines,1):
        if not line.strip():continue
        try: event=json.loads(line)
        except json.JSONDecodeError as e:raise ValueError(f'Invalid JSON at line {n}') from e
        if event.get('origin')!='iot_benchmark_generate_query_workload':
            raise ValueError(f'Line {n}: origin marker missing; not an official generator sample')
        op=event.get('operation')
        if op not in OPERATIONS:raise ValueError(f'Line {n}: unsupported operation {op}')
        q=event.get('query')
        if not isinstance(q,dict):raise ValueError(f'Line {n}: missing serialized query object')
        qid=OPERATIONS[op];counts[qid]+=1
        if qid=='Q12':
            children=q.get('childRangeQueries');setop=str(q.get('setOpType','')).lower()
            if setop not in {'union','intersect','except'} or not isinstance(children,list) or len(children)<2:
                raise ValueError(f'Line {n}: invalid native Q12 operands/type')
            if any(not isinstance(x,dict) or 'deviceSchema' not in x for x in children):
                raise ValueError(f'Line {n}: invalid Q12 child query')
            for child_index, child in enumerate(children):
                ds=child['deviceSchema']
                if not isinstance(ds,list) or not ds:
                    raise ValueError(f'Line {n}: Q12 child {child_index} has no deviceSchema list')
                for device_index, device in enumerate(ds):
                    if not isinstance(device,dict) or '$ref' in device:
                        raise ValueError(f'Line {n}: Q12 child {child_index} device {device_index}'
                                         ' is a Fastjson $ref; regenerate with the fixed Java exporter')
                    ss=device.get('sensors')
                    if not isinstance(ss,list) or not ss:
                        raise ValueError(f'Line {n}: Q12 child {child_index} device {device_index}'
                                         ' has no complete sensors list')
                    for sensor_index, sensor in enumerate(ss):
                        if not isinstance(sensor,dict) or '$ref' in sensor:
                            raise ValueError(f'Line {n}: Q12 child {child_index} device {device_index}'
                                             f' sensor {sensor_index} is a Fastjson $ref;'
                                             ' regenerate with the fixed Java exporter')
                        if not isinstance(sensor.get('name'),str) or not sensor['name']:
                            raise ValueError(f'Line {n}: Q12 child {child_index} device {device_index}'
                                             f' sensor {sensor_index} missing name')
        else:
            ds=q.get('deviceSchema')
            if not isinstance(ds,list) or not ds:raise ValueError(f'Line {n}: missing native deviceSchema')
            for d in ds:
                if not isinstance(d,dict) or not d.get('sensors'):
                    raise ValueError(f'Line {n}: missing native sensors')
        byq.setdefault(qid,[]).append(event)
    if expected is not None:
        missing=set(expected)-set(byq)
        if missing:raise ValueError('Missing official query classes: '+', '.join(sorted(missing)))
    return byq,dict(counts)

def main():
    p=argparse.ArgumentParser(description='Audit native IoT Benchmark Java query-object JSONL; not SQL replay')
    p.add_argument('--capture',type=Path,required=True)
    p.add_argument('--require-all-12',action='store_true')
    p.add_argument('--output',type=Path,default=Path('outputs/official_capture_audit.json'))
    a=p.parse_args()
    expected=[f'Q{i}' for i in range(1,13)] if a.require_all_12 else None
    events,counts=validate(a.capture,expected)
    report={'source':str(a.capture),'source_sha256':hashlib.sha256(a.capture.read_bytes()).hexdigest(),
            'official_generator_query_object_sample':True,'official_sql_captured':False,
            'native_scheduling_sequence_replayed':False,'instances_by_class':counts,
            'classes_found':sorted(events,key=lambda x:int(x[1:])),
            'blocker':'SQL projection semantics, especially Q8/Q12, require target-dialect verification',
            'benchmark_executed':False}
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print('Audited query objects:',counts,'\nAudit:',a.output)
if __name__=='__main__':main()


def representative_queries(events_by_class, data, scenario, operations, capture_path):
    """Bind ONE real Java-generated parameter object per class to the existing 12-class pilot.

    IoTDB table-mode getTimeWhereClause uses an inclusive <= upper bound.
    The SchemaLens internal model uses exclusive end, hence native end+1.
    Important: this is a representative-class bridge, NOT a replay of event frequency/order.
    For that, a later workload-level redesign is required. Inputs are checked fail-closed.
    """
    sha=hashlib.sha256(capture_path.read_bytes()).hexdigest()
    devices=set(data['devices']);sensors=set(data['sensors']);min_t=data['min_timestamp'];max_t=data['max_timestamp']+1

    def parse_devices(objs,label):
        if not isinstance(objs,list) or not objs:raise ValueError(label+': missing device schemas')
        dev=[];selected_sensors=None
        for obj in objs:
            if not isinstance(obj,dict):raise ValueError(label+': invalid device schema')
            name=obj.get('device')
            if not isinstance(name,str) or name not in devices:
                raise ValueError(label+': selected device is absent from imported native CSV: '+str(name))
            present=[]
            for s in obj.get('sensors',[]):
                if isinstance(s,dict) and '$ref' in s:
                    raise ValueError(label+': unresolved Fastjson $ref; regenerate with fixed Java exporter')
                n=s.get('name') if isinstance(s,dict) else None
                if not isinstance(n,str) or not n:
                    raise ValueError(label+': missing complete native Sensor DTO/name')
                if n not in sensors:raise ValueError(label+': unknown sensor: '+str(n))
                present.append(n)
            if not present or len(set(present))!=len(present):raise ValueError(label+': invalid sensors')
            if selected_sensors is not None and present!=selected_sensors:
                raise ValueError(label+': heterogeneous sensor projections not yet implemented')
            selected_sensors=present
            dev.append(name)
        return list(dict.fromkeys(dev)),selected_sensors

    result=[]
    for op in operations:
        qid=op['id'];e=events_by_class[qid][0];native=e['query']
        children=None
        if qid=='Q12':
            children=[]
            for ch in native['childRangeQueries']:
                ds,ss=parse_devices(ch.get('deviceSchema'),'Q12 child')
                children.append({'devices':ds,'sensors':ss,'start_ms':int(ch['startTimestamp']),
                                 'end_ms_exclusive':int(ch['endTimestamp'])+1})
            if len({tuple(x['sensors']) for x in children})!=1:
                raise ValueError('Q12 operands have different projections')
            all_devs=list(dict.fromkeys(d for ch in children for d in ch['devices']))
            selected=children[0]['sensors']
            start=min(x['start_ms'] for x in children);end=max(x['end_ms_exclusive'] for x in children)
        else:
            all_devs,selected=parse_devices(native['deviceSchema'],qid)
            start=int(native.get('startTimestamp',min_t));end=int(native['endTimestamp'])+1 if 'endTimestamp' in native else max_t
        if start>=end:raise ValueError(qid+': empty or inverted native time range')
        agg=str(native.get('aggFun','')).lower() if op['aggregate'] else None
        if op['aggregate'] and agg not in {'count','sum','avg','min','max'}:
            raise ValueError(qid+': unsupported native aggregation '+str(agg))
        v=native.get('valueThreshold') if qid in {'Q3','Q5','Q6','Q10'} else None
        gran=int(native['granularity']) if qid in {'Q7','Q11'} else None
        if gran is not None and gran<=0:raise ValueError(qid+': invalid native date_bin width')
        set_op=str(native['setOpType']).lower() if qid=='Q12' else None
        q={'query_id':qid,'name':op['name'],'devices':all_devs,'sensors':selected,
           'start_ms':start,'end_ms_exclusive':end,
           'point_ms':int(native.get('timestamp',min_t)) if qid=='Q1' else min_t,
           'aggregation':agg,'value_lower':float(v) if v is not None else None,
           'group_by_ms':gran,'set_op':set_op,'set_children':children,
           'parameter_origin':'sample from official GenerateQueryWorkLoad Java API, not SQL or mixed-workload sequence',
           'capture_sha256':sha,'native_sample_index':e.get('sample_index'),
           'correctness_status':'parameter-object translated; dialect SQL equivalence not independently verified'}
        if not all(isinstance(x,int) and -(2**63)<=x<2**63 for x in (q['start_ms'],q['end_ms_exclusive'],q['point_ms'])):
            raise ValueError(qid+': timestamp outside signed 64-bit')
        result.append(q)
    return result
