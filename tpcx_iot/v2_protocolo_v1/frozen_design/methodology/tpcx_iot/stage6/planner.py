"""Stage 6 — Cassandra WritePlan generator.

Grounded in Stage 4 candidate specifications and Stage 5 read plans.
Neither reactivates families nor uses benchmark measurements. Every write
plan is conditional until complete-source data and live database checks exist.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from methodology.tpcx_iot.stage4.candidates import ROOT, read_json, sha256, write_json
from methodology.tpcx_iot.stage5.planner import build_plans, verify_stage4


@dataclass(frozen=True)
class WritePlan:
    plan_id: str
    query_id: str
    candidate_id: str
    family: str
    physical_table: str
    partition_key: tuple[str, ...]
    clustering_key: tuple[str, ...]
    column_order: tuple[str, ...]
    write_cql: str
    bucket_seconds: int | None
    role: str
    write_strategy: str
    identity: tuple[str, ...]
    requirements: tuple[str, ...]
    maintenance: tuple[str, ...]
    active_modifiers: tuple[str, ...]
    offline_reference_status: str = 'REFERENCE_ONLY'
    cassandra_verified: bool = False
    distributed_atomicity_proven: bool = False
    benchmarked: bool = False


def verify_source(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    root = root.resolve()
    specs, audit4 = verify_stage4(root)
    src_dir = root / 'outputs/tpcx_iot_stage5'
    p_manifest = src_dir / '23_queryplan_manifest.json'
    p_plans = src_dir / '20_queryplans.json'
    p_coverage = src_dir / '21_read_coverage.json'
    p_probes = src_dir / '22_offline_equivalence_probes.json'
    for p in (p_manifest, p_plans, p_coverage, p_probes):
        if not p.is_file():
            raise FileNotFoundError(f'Stage 5 input missing: {p}')
    manifest = read_json(p_manifest)
    if manifest.get('stage') != 5 or manifest.get('engine') != 'Apache Cassandra':
        raise ValueError('Wrong Stage 5 provenance')
    if manifest['source_hashes'] != audit4:
        raise ValueError('Stage 5 no longer matches frozen Stage 4 input hashes')
    if (manifest.get('benchmark_results_used') is not False or
            manifest.get('operational_coverage_claimed') is not False or
            manifest.get('cassandra_cluster_used') is not False):
        raise ValueError('Unexpected prior execution/results')
    expected = json.loads(json.dumps([asdict(p) for p in build_plans(specs)]))
    plans = read_json(p_plans)
    if plans != expected:
        raise ValueError('Stage 5 QueryPlans drifted from verified Stage 4 candidates')
    coverage = read_json(p_coverage)
    if len(coverage) != 2 * len(specs) or any(x['eligible_for_complete_workload_HBench'] for x in coverage):
        raise ValueError('Stage 5 coverage unexpectedly changed')
    probes = read_json(p_probes)
    if len(probes) != 25 or not all(x['passed'] for x in probes):
        raise ValueError('Stage 5 offline probes incomplete')
    return specs, plans, {
        'stage4_candidate_specs_sha256': sha256(root/'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json'),
        'stage5_plans_sha256': sha256(p_plans),
        'stage5_coverage_sha256': sha256(p_coverage),
        'stage5_probes_sha256': sha256(p_probes),
        'stage5_manifest_sha256': sha256(p_manifest),
    }


def verify_policy(policy: dict[str, Any]) -> None:
    if policy.get('policy_version') != 'tpcx-iot-cassandra-stage6-v1':
        raise ValueError('Stage 6 policy version mismatch')
    expected = {
        'research_only': True,
        'source_profile': 'sensor-name-only/raw_read',
        'logical_operation': 'INSERT',
        'identity_policy': 'stable_uuid_or_deterministic_uuid5_from_unique_source_record_id',
        'replay_semantics': 'identical_full_event_is_noop',
        'same_identity_different_payload': 'reject_conflicting_rewrite',
        'require_external_station_roster': True,
        'do_not_auto_select_ttl': True,
        'do_not_auto_select_compaction': True,
        'do_not_auto_select_bucket_width': True,
        'do_not_use_logged_cross_partition_batches': True,
        'use_benchmark_to_choose_candidates': False,
        'allow_live_driver': False,
    }
    for key, value in expected.items():
        if policy.get(key) != value:
            raise ValueError(f'Stage 6 unsafe policy setting: {key}')


def verify_insert(spec: dict[str, Any]) -> tuple[str, ...]:
    """The frozen Stage 4 INSERT is checked exactly, not regenerated differently."""
    table, columns = spec['table'], tuple(spec['columns'])
    if not re.fullmatch('[a-z][a-z0-9_]*', table):
        raise ValueError('Unsafe CQL table')
    if not columns or len(columns) != len(set(columns)) or any(
        not re.fullmatch('[a-z][a-z0-9_]*', name) for name in columns
    ):
        raise ValueError('Invalid CQL column name')
    expected = ('INSERT INTO '+table+' ('+', '.join(columns)+') VALUES ('+
                ', '.join('?' for _ in columns)+');')
    if spec['write_cql_template'] != expected or spec['write_cql_template'].count('?') != len(columns):
        raise ValueError('Stage 4 INSERT does not match required column order')
    if set(spec['partition_key'] + spec['clustering_key']) - set(columns):
        raise ValueError('Missing primary key for insertion')
    return columns


def build_write_plans(specs: list[dict[str,Any]], read_plans: list[dict[str, Any]]) -> list[WritePlan]:
    read_by_id = {p['candidate_id']: p for p in read_plans}
    if len(read_by_id) != len(read_plans) or set(read_by_id) != {s['candidate_id'] for s in specs}:
        raise ValueError('Each candidate must have a corresponding read plan')
    output=[]
    for spec in specs:
        cid, fam = spec['candidate_id'], spec['family']
        if fam not in {'TS0', 'TS1', 'TS3'}:
            raise ValueError('Unexpected unactivated temporal family')
        columns=verify_insert(spec)
        rp = read_by_id[cid]
        if rp['physical_table'] != spec['table'] or rp['family'] != fam:
            raise ValueError('Read/Write physical identity mismatch')
        role = 'materialized_sensor_projection' if fam == 'TS3' else 'canonical_series_table'
        requirements = [
            'full_immutable_input_event_with_source_unique_identity',
            'event_time_is_signed_int64_epoch_milliseconds_utc',
            'external_complete_station_roster_assertion_with_provenance',
            'same_full_event_replay_must_not_duplicate',
            'reject_conflicting_identity_updates',
            'all_selected_table_writes_required_for_complete_deployment',
        ]
        maintenance = [
            'floor_divide_event_time_into_UTC_bucket_when_applicable',
            'bind_INSERT_values_in_exact_frozen_Stage4_column_order',
            'retain_event_reading_and_full_padding',
            'use_deterministic_UUID_if_source_record_id_is_present',
            'retry_same_CQL_mutations_after_partial_failure',
        ]
        if fam=='TS3':
            maintenance.append('duplicate_each_input_event_across_all_stations_for_sensor_name_access')
            maintenance.append('require_canonical_source_and_recovery_protocol_in_complete_deployment')
        if fam=='TS0':
            maintenance.append('document_unbounded_per_stream_partition_size_risk')
        output.append(WritePlan(
            plan_id=f'wp__q_ingest__{cid}', query_id='Q_INGEST',
            candidate_id=cid, family=fam, physical_table=spec['table'],
            partition_key=tuple(spec['partition_key']), clustering_key=tuple(spec['clustering_key']),
            column_order=columns, write_cql=spec['write_cql_template'],
            bucket_seconds=spec['bucket_seconds'], role=role,
            write_strategy='raw_idempotent_upsert_plus_projection' if fam=='TS3' else 'raw_idempotent_upsert',
            identity=('driver_system_id', 'sensor_name', 'event_ts', 'event_id'),
            requirements=tuple(requirements), maintenance=tuple(maintenance),
            active_modifiers=tuple(spec['active_modifiers']),
        ))
    return output


def generate(root:Path=ROOT, output_dir:Path|None=None)->dict[str,Any]:
    root=root.resolve()
    out=(output_dir or root/'outputs/tpcx_iot_stage6').resolve()
    if (out==root or out.is_relative_to(root/'inputs') or
            out.is_relative_to(root/'outputs/tpcx_iot_stage1_3') or
            out.is_relative_to(root/'outputs/tpcx_iot_stage4') or
            out.is_relative_to(root/'outputs/tpcx_iot_stage5')):
        raise ValueError('Output overlaps frozen upstream artifacts')
    specs, reads, hashes = verify_source(root)
    policy_path=root/'config/tpcx_stage6_write_policy.json'
    policy=read_json(policy_path)
    verify_policy(policy)
    plans=build_write_plans(specs, reads)
    from .offline import run_offline_suite
    probes, examples=run_offline_suite(plans, reads)
    if not all(p['passed'] for p in probes):
        raise ValueError('Offline WritePlan probe failed; refusing publication')
    out.mkdir(parents=True, exist_ok=True)
    write_json(out/'30_writeplans.json',[asdict(p) for p in plans])
    write_json(out/'31_write_coverage.json', [
        {
            'candidate_id': p.candidate_id, 'family': p.family,
            'query_id': 'Q_INGEST', 'logical_insert_write_plan_present': True,
            'offline_write_fixture_passed': True, 'live_write_coverage_verified': False,
            'write_coverage_status': 'CONDITIONAL_OFFLINE_ONLY',
            'complete_deployment_proven': False, 'eligible_for_complete_workload_HBench': False,
        } for p in plans
    ])
    write_json(out/'32_offline_write_probes.json', probes)
    write_json(out/'34_example_mutations.json', examples)
    write_json(out/'33_writeplan_manifest.json', {
        'stage':6, 'engine':'Apache Cassandra',
        'benchmark':'TPCx-IoT 2.1.2 adapted research profile',
        'source_assumption':'sensor_name_only; unverified against official Java kit',
        'candidate_count': len(plans), 'write_plan_count':len(plans),
        'conditional_write_plan_count':len(plans),
        'offline_probe_count':len(probes), 'offline_probes_passed':sum(p['passed'] for p in probes),
        'families':sorted({p.family for p in plans}),
        'write_fanout_if_all_five_are_materialized':len(plans),
        'note_on_fanout':'Demonstration only; Stage8 must select compatible complete deployments',
        'reference_maintenance_only':True,
        'source_sha256': {**hashes,'stage6_policy_sha256':sha256(policy_path)},
        'stage3_reexecuted':False, 'activation_rules_changed':False,
        'upstream_outputs_modified':False,'benchmark_results_used':False,
        'live_cassandra_executed':False,'distributed_atomicity_proven':False,
        'operational_coverage_claimed':False, 'hbench_selected':False,
        'next':'Stage7 analytical physical costs and feasibility, Stage8 compatible covers and HBench',
    })
    return read_json(out/'33_writeplan_manifest.json')
