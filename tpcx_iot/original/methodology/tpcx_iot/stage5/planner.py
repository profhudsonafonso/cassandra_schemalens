"""Stage 5: QueryPlan construction and conditional read coverage.

Inputs are immutable Stage 3 evidence and Stage 4 candidate specifications.
No family activation, write-plan synthesis, materialization, or benchmarking.
An executable CQL plan is NOT proof of physical data maintenance or live correctness.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
from . import __name__ as _module_name
from methodology.tpcx_iot.stage4.candidates import (
    ROOT, prototypes, read_json, sha256, verify_inputs, write_json,
)

QUERY_ID = 'Q_READ_2_WINDOWS'
INGEST_ID = 'Q_INGEST'
REQUIRED_COLS = ('driver_system_id', 'sensor_name', 'event_ts', 'event_id', 'sensor_reading', 'padding')


@dataclass(frozen=True)
class QueryPlan:
    plan_id: str
    query_id: str
    candidate_id: str
    family: str
    physical_table: str
    access_mode: str
    time_bucket_seconds: int | None
    read_cql: str
    partition_key: tuple[str, ...]
    clustering_key: tuple[str, ...]
    logical_time_ranges: int
    expected_window_lengths_seconds: tuple[int, int]
    interval_bounds: str
    merge_identity: tuple[str, ...]
    operators: tuple[str, ...]
    read_preconditions: tuple[str, ...]
    read_coverage_status: str
    write_coverage_status: str = 'STAGE6_UNVERIFIED'
    live_coverage_status: str = 'STAGE9_UNVERIFIED'
    benchmark_used_for_selection: bool = False


def verify_read_cql(spec: dict[str, Any]) -> None:
    """Fail-closed verification of our tightly constrained Cassandra SELECT form.

    Not a general-purpose CQL parser or proof of execution against Cassandra.
    """
    cql = spec['read_cql_template']
    table = spec['table']
    if not re.fullmatch(
        rf'SELECT\s+driver_system_id, sensor_name, event_ts, event_id, sensor_reading, padding\s+'
        rf'FROM\s+{re.escape(table)}\s+WHERE\s+.+;', cql,
        flags=re.IGNORECASE,
    ):
        raise ValueError(f'{table}: incorrect read SELECT/projection')
    if any(x in cql.upper() for x in ('ALLOW FILTERING', 'CREATE INDEX', 'TOKEN(', 'GROUP BY', ' OR ', ' IN ')):
        raise ValueError('Unbounded/unsupported CQL filtering')
    where = re.split(r'\s+WHERE\s+', cql, flags=re.I)[1].removesuffix(';')
    predicates = where.split(' AND ')
    expected = [f'{k} = ?' for k in spec['partition_key']]
    expected.extend(['event_ts >= ?', 'event_ts <= ?'])
    if predicates != expected:
        raise ValueError(f'{table}: predicates do not match complete PK plus time range')
    ck = spec['clustering_key']
    if ck[0] != 'event_ts' or len(set(ck)) != len(ck):
        raise ValueError(f'{table}: wrong time clustering key')
    if spec['query_filter_mode'] not in ('station_and_sensor','sensor_only'):
        raise ValueError('Unknown access mode')
    if cql.count('?') != len(spec['partition_key'])+2:
        raise ValueError('Placeholder count mismatch')


def validate_sql_intent(root: Path) -> str:
    """Check the exact scoped Stage1-3 frozen logical SQL, NOT arbitrary SQL."""
    path=root/'inputs/workload/sql/Q_READ_2_WINDOWS.sql'
    snapshot=root/'outputs/tpcx_iot_stage1_3/inputs_snapshot/inputs/workload/sql/Q_READ_2_WINDOWS.sql'
    if not snapshot.is_file() or path.read_bytes()!=snapshot.read_bytes():
        raise ValueError('Logical query changed after source snapshot; revalidate assumptions')
    sql=path.read_text(encoding='utf-8')
    s=' '.join(sql.lower().split())
    expected=(
        'select r.event_ts, r.sensor_reading from reading r '
        'where r.sensor_name = :sensor_name '
        'and ((r.event_ts between :ti1_start and :ti1_end) '
        'or (r.event_ts between :ti2_start and :ti2_end));'
    )
    if s != expected:
        raise ValueError('Logical query not the frozen raw two-window access contract')
    return sha256(path)


def verify_stage4(root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    frozen=root/'inputs/stage3_frozen'
    policy=root/'config/tpcx_stage4_cassandra_policy.json'
    rows,audit=verify_inputs(root,frozen,policy)
    manifest_path=root/'outputs/tpcx_iot_stage4/14_stage4_manifest.json'
    m=read_json(manifest_path)
    if m.get('stage')!=4 or m.get('engine')!='Apache Cassandra' or m.get('candidate_count')!=5:
        raise ValueError('Stage4 manifest incompatible')
    if m.get('used_benchmark_results') is not False or m.get('benchmark_executed') is not False:
        raise ValueError('Stage4 artifact tainted by benchmark results')
    if m['input_sha256'] != audit['hashes']:
        raise ValueError('Stage4 input hashes drifted; run Stage4 explicitly')
    expected=prototypes(read_json(policy))
    selected=[asdict(s) for cid,s in sorted(expected.items()) if s.family in {'TS0','TS1','TS3'}]
    specs_path=root/'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json'
    specs=read_json(specs_path)
    if specs!=json.loads(json.dumps(selected)):
        raise ValueError('Candidate specification drifted from frozen Stage4 generation')
    if m['candidate_ids']!=[s['candidate_id'] for s in specs]:
        raise ValueError('Manifest/candidate identifier mismatch')
    primitives=read_json(root/'outputs/tpcx_iot_stage4/12_query_read_primitives.json')
    if len(primitives)!=len(specs):raise ValueError('Read primitive count mismatch')
    for spec, prim in zip(specs, primitives):
        if (prim['candidate_id']!=spec['candidate_id'] or
            prim['query_cql_primitive']!=spec['read_cql_template'] or
            prim['status']!='NO_EXECUTABLE_PLAN_YET'):
            raise ValueError('Read primitive drifted')
        verify_read_cql(spec)
        ddl=root/'outputs/tpcx_iot_stage4/cql/schemas'/f"{spec['candidate_id']}.cql"
        if not ddl.is_file(): raise ValueError('Missing CQL DDL')
        if 'ALLOW FILTERING' in ddl.read_text().upper(): raise ValueError('Unsafe DDL')
    origins=read_json(root/'outputs/tpcx_iot_stage4/11_query_candidate_map.json')
    if set(x['query_id'] for x in origins)!={INGEST_ID, QUERY_ID}:
        raise ValueError('Missing operation origin')
    family_to_ids={k:sorted(s['candidate_id'] for s in specs if s['family']==k)
                   for k in ('TS0','TS1','TS3')}
    read_origin=next(x for x in origins if x['query_id']==QUERY_ID)
    if read_origin['candidate_ids_by_family']!=family_to_ids:
        raise ValueError('Read activation changed')
    if set(rows['activation'])!={INGEST_ID,QUERY_ID}:
        raise ValueError('Unexpected workload')
    return specs,{'stage4_manifest_sha256':sha256(manifest_path),
                  'stage4_specs_sha256':sha256(specs_path),
                  'stage4_primitives_sha256':sha256(root/'outputs/tpcx_iot_stage4/12_query_read_primitives.json'),
                  'logical_sql_sha256':validate_sql_intent(root),
                  'frozen_stage3_sha256':audit['hashes']}


def build_plans(specs: list[dict[str,Any]]) -> list[QueryPlan]:
    result=[]
    for spec in specs:
        verify_read_cql(spec)
        mode=spec['query_filter_mode']
        requirements=['two_explicit_5s_inclusive_windows', 'stable_event_identity',
                      'all_candidate_rows_materialized', 'correct_utc_epoch_milliseconds']
        ops=['bind_temporal_windows', 'split_into_intersecting_partitions']
        if mode=='station_and_sensor':
            requirements+=['complete_station_roster', 'complete_roster_provenance']
            ops.append('fan_out_to_all_stations')
        elif mode=='sensor_only':
            requirements+=['ts3_projection_correctly_maintained']
            ops.append('direct_sensor_lookup')
        if spec['bucket_seconds'] is not None:ops.append('enumerate_utc_bucket_starts_floor')
        ops+=['read_all_pages_for_each_cql_request', 'merge_two_windows_or_semantics',
              'deduplicate_overlap_by_physical_event_identity',
              'project_event_ts_and_sensor_reading_preserving_multiplicity', 'canonical_sort_for_validation_only']
        result.append(QueryPlan(
            plan_id=f"qp__{QUERY_ID.lower()}__{spec['candidate_id']}",
            query_id=QUERY_ID,candidate_id=spec['candidate_id'],family=spec['family'],
            physical_table=spec['table'],access_mode=mode,
            time_bucket_seconds=spec['bucket_seconds'],read_cql=spec['read_cql_template'],
            partition_key=tuple(spec['partition_key']),clustering_key=tuple(spec['clustering_key']),
            logical_time_ranges=2,expected_window_lengths_seconds=(5,5),
            interval_bounds='inclusive',merge_identity=('driver_system_id','sensor_name','event_ts','event_id'),
            operators=tuple(ops),read_preconditions=tuple(requirements),
            read_coverage_status='CONDITIONAL_OFFLINE_ONLY',
        ))
    if len(result)!=5:raise ValueError('Expected five read plans')
    return result


def generate(root: Path=ROOT, output_dir: Path|None=None) -> dict[str,Any]:
    root=root.resolve()
    out=(output_dir or root/'outputs/tpcx_iot_stage5').resolve()
    if out==root or out.is_relative_to(root/'inputs') or out.is_relative_to(root/'outputs/tpcx_iot_stage4'):
        raise ValueError('Output overlaps immutable upstream data')
    specs,audit=verify_stage4(root)
    plans=build_plans(specs)
    # Imported only after source checks; benchmark-free deterministic probes.
    from .offline import run_offline_suite
    probes=run_offline_suite(plans)
    if not all(item['passed'] for item in probes):
        raise ValueError('Offline equivalence probe failed; refusing to publish read plans')
    out.mkdir(parents=True,exist_ok=True)
    write_json(out/'20_queryplans.json',[asdict(p) for p in plans])
    write_json(out/'21_read_coverage.json',[
        {'query_id':QUERY_ID,'candidate_id':p.candidate_id,'family':p.family,
         'originated_from_query':True,'read_structural_coverage':True,
         'read_coverage':'CONDITIONAL_OFFLINE_ONLY', 'requirements':p.read_preconditions,
         'offline_equivalence_fixture_passed':True,
         'dataset_complete':False,'cassandra_executed':False,'write_coverage':'STAGE6_UNVERIFIED',
         'eligible_for_complete_workload_HBench':False}
        for p in plans]+[
        {'query_id':INGEST_ID,'candidate_id':p.candidate_id,'family':p.family,
         'originated_from_query':p.family=='TS0','read_structural_coverage':False,
         'read_coverage':'NOT_APPLICABLE_WRITE_OPERATION','requirements':['stage6_write_plan'],
         'offline_equivalence_fixture_passed':False,'dataset_complete':False,'cassandra_executed':False,
         'write_coverage':'STAGE6_UNVERIFIED','eligible_for_complete_workload_HBench':False}
        for p in plans])
    write_json(out/'22_offline_equivalence_probes.json',probes)
    write_json(out/'23_queryplan_manifest.json',{
        'stage':5,'engine':'Apache Cassandra','benchmark':'TPCx-IoT 2.1.2 adapted research profile',
        'query_count':2,'read_query_count':1,'write_query_count':1,
        'read_plan_count':len(plans),'conditional_read_plan_count':len(plans),
        'offline_probe_count':len(probes),'offline_probes_passed':sum(x['passed'] for x in probes),
        'conditionally_covered_families':['TS0','TS1','TS3'],
        'source_assumption':'sensor_name_only; unverified against official Java kit',
        'logical_intervals':'two 5-second closed intervals; OR semantics; no order guarantee',
        'source_hashes':audit, 'stage3_reexecuted':False,'activation_rules_changed':False,
        'benchmark_results_used':False,'cassandra_cluster_used':False,
        'operational_coverage_claimed':False,'write_plans_completed':False,
        'hbench_selected':False,'next':'Stage6 write plans and complete roster/projection maintenance'
    })
    return read_json(out/'23_queryplan_manifest.json')
