"""Pure-Python reference writer for Apache Cassandra Stage 6.

This module neither opens a Cassandra connection nor provides durable
transactions. It simulates complete, immutable-event inserts and all selected
physical projections; a replayable mutation list demonstrates convergence after
partial writes in an idealized single-writer environment.
"""
from __future__ import annotations

import copy
import json
import math
from dataclasses import dataclass
from typing import Any, Iterable
from uuid import UUID, uuid5, NAMESPACE_URL

from methodology.tpcx_iot.stage5.runtime import (
    ClosedWindow, bind_read_requests, merge_read_results, normalize_station_roster,
)


class PartialWriteFailure(RuntimeError):
    """An injected failure after the specified number of mutations were applied."""


def event_uuid(station: str, sensor: str, time_ms: int, source_record_id: str) -> UUID:
    """Reproducible UUID from an EXTERNALLY unique source-record identifier.

    Timestamp is not enough: different events may share the same instant.
    Do not fabricate source_record_id from timestamp or sensor reading.
    """
    if not source_record_id or not isinstance(source_record_id,str) or not source_record_id.strip():
        raise ValueError('Source record ID must be a non-empty unique string')
    canonical=json.dumps([station,sensor,time_ms,source_record_id],
                         separators=(',', ':'),ensure_ascii=False)
    return uuid5(NAMESPACE_URL, 'schemalens-tpcx-iot-stage6:'+canonical)


def normalize_event(record: dict[str,Any]) -> dict[str,Any]:
    if not isinstance(record,dict):raise ValueError('Event must be a mapping')
    needed = ('driver_system_id','sensor_name','event_ts','sensor_reading','padding')
    if any(k not in record for k in needed):raise ValueError('Incomplete source event')
    station,sensor=record['driver_system_id'],record['sensor_name']
    if any(not isinstance(v,str) or not v.strip() for v in (station,sensor)):
        raise ValueError('Station and sensor must be nonempty strings')
    ts=record['event_ts']
    if type(ts) is not int or not -(2**63)<=ts<2**63:
        raise ValueError('Timestamp must be signed int64 UTC epoch milliseconds')
    value=record['sensor_reading']
    if type(value) not in (int,float) or not math.isfinite(value):
        raise ValueError('Sensor reading must be finite numeric, not bool or NaN')
    if not isinstance(record['padding'],str):
        raise ValueError('Original padding must be a string')
    if record.get('operation','INSERT') != 'INSERT':
        raise ValueError('This profile supports append/INSERT only (no updates/deletes)')
    eid=record.get('event_id')
    sid=record.get('source_record_id')
    deterministic=event_uuid(station,sensor,ts,sid) if sid is not None else None
    if eid is None and deterministic is None:
        raise ValueError('Stable event_id UUID or externally unique source_record_id required')
    try:
        event_id=UUID(str(eid)) if eid is not None else deterministic
    except (ValueError,TypeError,AttributeError) as exc:
        raise ValueError('Invalid event_id UUID') from exc
    if deterministic is not None and event_id != deterministic:
        raise ValueError('Explicit event_id differs from deterministic source record ID')
    return dict(driver_system_id=station,sensor_name=sensor,event_ts=ts,event_id=event_id,
                sensor_reading=float(value),padding=record['padding'])


def bucket_start(time_ms:int,bucket_seconds:int|None)->int|None:
    if bucket_seconds is None:return None
    if type(bucket_seconds) is not int or bucket_seconds<=0:
        raise ValueError('Invalid candidate bucket width')
    width=bucket_seconds*1000
    return (time_ms//width)*width


def physical_row(plan:dict[str,Any],event:dict[str,Any])->dict[str,Any]:
    columns=plan['column_order']
    row=dict(event)
    bucket=bucket_start(event['event_ts'],plan['bucket_seconds'])
    if bucket is not None:row['bucket_start_ms']=bucket
    if set(row)!=set(columns):raise ValueError('Stage4/6 physical column divergence')
    return {c:row[c] for c in columns}


def primary_key(plan:dict[str,Any],row:dict[str,Any])->tuple:
    return tuple(row[name] for name in (*plan['partition_key'],*plan['clustering_key']))


@dataclass(frozen=True)
class AuthoritativeRoster:
    """Caller-supplied COMPLETE roster assertion, not independently verified."""
    stations:tuple[str,...]
    provenance:str
    asserted_complete:bool

    def __post_init__(self):
        cleaned=normalize_station_roster(list(self.stations),self.asserted_complete,self.provenance)
        object.__setattr__(self,'stations',tuple(cleaned))


class ReferenceMaterializer:
    def __init__(self,write_plans: list[dict[str,Any]],roster:AuthoritativeRoster,
                 candidate_ids:Iterable[str]|None=None):
        if not isinstance(roster,AuthoritativeRoster):raise ValueError('Require explicit roster')
        by_id={p['candidate_id']:p for p in write_plans}
        if len(by_id)!=len(write_plans) or not by_id:
            raise ValueError('WritePlan IDs must be unique')
        chosen=set(by_id) if candidate_ids is None else set(candidate_ids)
        if not chosen or not chosen<=set(by_id):raise ValueError('Invalid candidate selection')
        self.plans={cid:by_id[cid] for cid in sorted(chosen)}
        self.roster=roster
        self.events:dict[tuple,dict]={}
        self.uuid_to_identity:dict[UUID,tuple]={}
        self.rows:dict[str,dict[tuple,dict]]={cid:{} for cid in self.plans}

    def apply(self,records:Iterable[dict[str,Any]])->list[dict[str,Any]]:
        """Stage a batch; rejected batches do not change any in-memory state.

        Repeated identical inserts yield zero mutations. Conflicting same-key
        rewrites are rejected. This is REFERENCE behavior, not Cassandra atomicity.
        """
        proposed={}
        local_uuids={}
        roster=set(self.roster.stations)
        for raw in records:
            ev=normalize_event(raw)
            if ev['driver_system_id'] not in roster:
                raise ValueError('Event station missing from asserted complete station roster')
            key=tuple(ev[n] for n in ('driver_system_id','sensor_name','event_ts','event_id'))
            uuid=ev['event_id']
            prior_id=local_uuids.get(uuid,self.uuid_to_identity.get(uuid))
            if prior_id is not None and prior_id!=key:
                raise ValueError('A UUID was reused for a different physical event')
            local_uuids[uuid]=key
            prior=proposed.get(key,self.events.get(key))
            if prior is not None and prior!=ev:
                raise ValueError('Conflicting rewrite for immutable event identity')
            proposed[key]=ev
        new_events=[ev for key,ev in proposed.items() if key not in self.events]
        new_events.sort(key=lambda e:(e['event_ts'],e['driver_system_id'],e['sensor_name'],str(e['event_id'])))
        actions=[]
        # Generate all binding/validation before changing the reference state.
        additions={cid:{} for cid in self.plans}
        for ev in new_events:
            for cid,plan in self.plans.items():
                row=physical_row(plan,ev)
                pk=primary_key(plan,row)
                existing=self.rows[cid].get(pk,additions[cid].get(pk))
                if existing is not None and existing!=row:
                    raise ValueError('Physical PK collision; cannot maintain projection safely')
                additions[cid][pk]=row
                params=[row[name] for name in plan['column_order']]
                if len(params)!=plan['write_cql'].count('?'):
                    raise ValueError('Wrong frozen CQL value count')
                actions.append(dict(candidate_id=cid,family=plan['family'],table=plan['physical_table'],
                                    operation='INSERT',primary_key=pk,cql=plan['write_cql'],
                                    parameters=params,row=copy.deepcopy(row),
                                    execution_status='NOT_EXECUTED_REFERENCE_ONLY'))
        for key,ev in proposed.items():
            self.events.setdefault(key,ev)
            self.uuid_to_identity[ev['event_id']]=key
        for cid,insertions in additions.items():
            self.rows[cid].update(insertions)
        return actions

    def recovery_replay(self)->list[dict[str,Any]]:
        """Full replay from an IN-MEMORY immutable canonical ledger.

        Can repair missing writes on the modeled sink. Does NOT imply that a
        Cassandra TS3 projection can self-recover after losing its source.
        """
        out=[]
        for cid,plan in self.plans.items():
            for pk,row in sorted(self.rows[cid].items(),key=lambda item:repr(item[0])):
                out.append(dict(candidate_id=cid,family=plan['family'],table=plan['physical_table'],
                                operation='INSERT',primary_key=pk,cql=plan['write_cql'],
                                parameters=[row[n] for n in plan['column_order']],
                                row=copy.deepcopy(row),
                                execution_status='NOT_EXECUTED_REFERENCE_ONLY'))
        return out

    def mock_read(self,read_plan:dict[str,Any], *,sensor_name:str,
                  recent:ClosedWindow,historical:ClosedWindow)->tuple[list[dict],int]:
        """Perform Stage5 bound requests against projected in-memory PK indexes."""
        cid=read_plan['candidate_id']
        if cid not in self.rows:raise ValueError('Candidate not materialized')
        reqs=bind_read_requests(read_plan,sensor_name=sensor_name,recent=recent,
                                historical=historical,stations=list(self.roster.stations),
                                complete_station_roster=self.roster.asserted_complete,
                                roster_provenance=self.roster.provenance)
        index={}
        for row in self.rows[cid].values():
            partition=tuple(row[k] for k in read_plan['partition_key'])
            index.setdefault(partition,[]).append(row)
        batches=[]
        for req in reqs:
            partition=tuple(req['params'][:len(read_plan['partition_key'])])
            rows=[dict(r) for r in index.get(partition,())
                  if req['range_start_ms']<=r['event_ts']<=req['range_end_ms']]
            batches.append((req,rows))
        return merge_read_results(read_plan,batches,sensor_name=sensor_name,
                                  recent=recent,historical=historical),len(reqs)


class InMemoryMutationSink:
    """External physical row simulation with an injected partial-write failure."""
    def __init__(self):
        self.rows:dict[str,dict[tuple,dict]]={}
        self.total_attempts=0

    def apply(self,mutations:list[dict[str,Any]],fail_after:int|None=None)->int:
        if fail_after is not None and (type(fail_after) is not int or fail_after<0):
            raise ValueError('fail_after must be nonnegative integer')
        applied=0
        for action in mutations:
            if fail_after is not None and applied==fail_after:
                raise PartialWriteFailure(f'Injected failure after {applied} writes')
            if action['operation']!='INSERT' or 'INSERT INTO ' not in action['cql']:
                raise ValueError('Only INSERT supported in modeled sink')
            self.total_attempts+=1
            self.rows.setdefault(action['candidate_id'],{})[action['primary_key']]=copy.deepcopy(action['row'])
            applied+=1
        return applied

    def is_equal_to(self,model:ReferenceMaterializer)->bool:
        return all(self.rows.get(cid,{})==rows for cid,rows in model.rows.items()) and set(self.rows)<=set(model.rows)


def json_ready(value: Any)->Any:
    """Serialize offline UUID/tuple parameters; driver must bind real UUID objects."""
    if isinstance(value,UUID):return str(value)
    if isinstance(value,tuple):return [json_ready(v) for v in value]
    if isinstance(value,list):return [json_ready(v) for v in value]
    if isinstance(value,dict):return {k:json_ready(v) for k,v in value.items()}
    return value
