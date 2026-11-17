"""Stage 7: Cassandra structural-cost proxies for the frozen TPCx-IoT profile.

Preserves the DKE SchemaLens boundary: candidate identification/activation and
query/write routing are immutable; pre-benchmark evidence cannot claim actual
latency, achieved throughput, disk usage, operational coverage or HBench selection.
All estimates are functions of an explicitly hypothetical scenario.
"""
from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from methodology.tpcx_iot.stage4.candidates import ROOT, read_json, sha256, write_json
from methodology.tpcx_iot.stage6.planner import (build_write_plans, verify_policy,
    verify_source)

VERSION = 'tpcx-iot-cassandra-stage7-v1'
OUTPUT_NAMES = (
    '40_scenario_evidence.json','41_physical_footprint_proxies.json',
    '42_read_cost_proxies.json','43_write_cost_proxies.json',
    '44_feasibility_diagnostics.json','45_candidate_cost_matrix.csv',
    '46_cost_manifest.json',
)


def _int(policy: dict[str,Any], key: str, low: int, high: int) -> int:
    value = policy.get(key)
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{key} must be an integer in [{low}, {high}]')
    return value


def validate_policy(p: dict[str,Any]) -> None:
    guards = {
        'policy_version': VERSION, 'research_only': True,
        'scenario_is_measured': False, 'benchmark_results_used': False,
        'source_profile': 'sensor-name-only/raw_read', 'rank_candidates': False,
        'select_hbench': False,
        'station_roster_status': 'DECLARED_NOT_EXTERNALLY_VERIFIED',
        'system_compaction': 'NOT_SELECTED_M3_ACTIVE_ONLY',
        'retention_ttl': 'NOT_SELECTED',
    }
    for k, v in guards.items():
        if p.get(k) != v: raise ValueError(f'Unsafe Stage7 policy: {k}')
    for key in ('station_count','sensor_types_per_station','sample_interval_seconds_per_stream',
                'horizon_seconds','logical_record_bytes_assumed',
                'row_storage_overhead_bytes_assumed','partition_storage_overhead_bytes_assumed',
                'replication_factor_assumed','max_partition_rows_warning',
                'max_partition_bytes_warning','max_total_partition_count_warning',
                'max_cql_requests_per_logical_read_warning',
                'max_expected_rows_per_logical_read_warning'):
        _int(p,key,1,10**10)
    if p['station_count'] > 1000000 or p['sensor_types_per_station'] > 1000000:
        raise ValueError('Unreasonable cardinality for reproducible estimation')
    if p['sensor_types_per_station'] != 200:
        raise ValueError('Frozen experiment models exactly 200 sensor types (change protocol first)')
    if p['horizon_seconds'] % p['sample_interval_seconds_per_stream'] != 0:
        raise ValueError('Horizon must contain an integer number of samples per stream')
    if p['horizon_seconds'] > 86400*365:
        raise ValueError('Synthetic horizon limited to one year for deterministic estimates')
    if p['horizon_seconds'] < 1806:
        raise ValueError('Scenario too short for historical lookback')
    if p['logical_record_bytes_assumed'] < 128:
        raise ValueError('Record size assumption unrealistic')
    start = p.get('starting_epoch_ms_utc')
    if type(start) is not int or start % 1000 != 0:
        raise ValueError('Scenario start must be whole UTC second in milliseconds')
    if not isinstance(p.get('query_sensor_name'),str) or not p['query_sensor_name']:
        raise ValueError('Query sensor name required')
    windows = p.get('two_closed_query_windows_seconds_from_start')
    if not isinstance(windows,list) or len(windows)!=2:
        raise ValueError('Exactly two time windows required')
    for w in windows:
        if not isinstance(w,list) or len(w)!=2 or any(type(v) is not int for v in w):
            raise ValueError('Every window must be [start_second,end_second]')
        a,b=w
        if not (0 <= a < b <= p['horizon_seconds'] and b-a==5):
            raise ValueError('Each window must be inside horizon and exactly five seconds wide')
    newer,older=sorted(windows, key=lambda x:x[0],reverse=True)
    if older[1] >= newer[0]:
        raise ValueError('Two benchmark-style windows must be nonoverlapping')
    if newer[1] - older[0] > 1805:
        raise ValueError('Historical window violates the 1800-second lookback envelope')
    # The window profile is a synthetic example; it does not prove official driver equivalence.
    if not isinstance(p.get('notes'),list): raise ValueError('Missing explanatory assumptions')


def _verify_stage6(root: Path) -> tuple[list[dict[str,Any]],list[dict[str,Any]],list[dict[str,Any]],dict[str,str]]:
    specs, reads, hashes = verify_source(root)
    policy_path = root/'config/tpcx_stage6_write_policy.json'
    verify_policy(read_json(policy_path))
    folder = root/'outputs/tpcx_iot_stage6'
    paths = {n:folder/n for n in ('30_writeplans.json','31_write_coverage.json',
             '32_offline_write_probes.json','33_writeplan_manifest.json')}
    for path in paths.values():
        if not path.is_file(): raise FileNotFoundError(f'Missing Stage 6 artifact: {path}')
    m=read_json(paths['33_writeplan_manifest.json'])
    if (m.get('stage')!=6 or m.get('engine')!='Apache Cassandra' or
        m.get('source_sha256')!={**hashes,'stage6_policy_sha256':sha256(policy_path)} or
        m.get('benchmark_results_used') is not False or
        m.get('live_cassandra_executed') is not False or
        m.get('hbench_selected') is not False):
        raise ValueError('Stage 6 provenance changed; rerun and audit previous stage')
    expected = json.loads(json.dumps([asdict(p) for p in build_write_plans(specs,reads)]))
    writes=read_json(paths['30_writeplans.json'])
    if expected != writes: raise ValueError('WritePlans drifted from frozen candidate specifications')
    cov=read_json(paths['31_write_coverage.json'])
    if (len(cov)!=len(writes) or any(x.get('write_coverage_status')!='CONDITIONAL_OFFLINE_ONLY'
                                    or x.get('eligible_for_complete_workload_HBench') for x in cov)):
        raise ValueError('Stage6 coverage status unexpectedly changed')
    probes=read_json(paths['32_offline_write_probes.json'])
    if len(probes)!=32 or not all(x.get('passed') is True for x in probes):
        raise ValueError('Stage6 offline write probes are missing or failing')
    if set(x['candidate_id'] for x in specs)!=set(x['candidate_id'] for x in writes):
        raise ValueError('Write/read candidate-set mismatch')
    return specs,reads,writes,{
        **hashes,
        'stage6_writeplans_sha256':sha256(paths['30_writeplans.json']),
        'stage6_writecoverage_sha256':sha256(paths['31_write_coverage.json']),
        'stage6_offlineprobes_sha256':sha256(paths['32_offline_write_probes.json']),
        'stage6_manifest_sha256':sha256(paths['33_writeplan_manifest.json']),
    }


def _buckets_inclusive(a: int,b: int,width: int) -> list[int]:
    # a,b are absolute event timestamps in seconds; Python // is floor for negative time.
    return list(range(a//width,b//width+1))


def _model_partition_rows(s:dict[str,Any],p:dict[str,Any])->tuple[int,int,int]:
    """(partition_count, maximum rows in one partition, min rows for a full bucket).

    The horizon is assumed to be aligned to both the 60s and 3600s boundaries
    because the default scenario begins at epoch 0. For other epoch starts,
    compute exact first/last bucket occupation using integer sample timestamps.
    """
    width=s['bucket_seconds']
    stations=p['station_count']; sensors=p['sensor_types_per_station']
    interval=p['sample_interval_seconds_per_stream']
    horizon=p['horizon_seconds']; begin=p['starting_epoch_ms_utc']//1000
    sample_count=horizon//interval
    if width is None:
        return (stations*sensors,sample_count,sample_count)
    # Values below use integer arithmetic and are exact for the uniform synthetic sequence.
    # Number of active buckets can vary from simple ceil(horizon / width) if phase shifted.
    # Count a single stream's samples per physical bucket, including partial edge buckets.
    per_bucket: dict[int,int]={}
    # Fast arithmetic: nth sample is begin+n*interval; obtain the first and last bucket,
    # then calculate index ranges for each bucket without enumerating all events.
    first=begin//width; last=(begin+(sample_count-1)*interval)//width
    for bucket in range(first,last+1):
        lo=max(0,(bucket*width-begin+interval-1)//interval)
        hi=min(sample_count-1,((bucket+1)*width-1-begin)//interval)
        if hi>=lo: per_bucket[bucket]=hi-lo+1
    if not per_bucket: raise ValueError('No sample buckets')
    source_partition_multiplicity=stations*sensors if s['query_filter_mode']=='station_and_sensor' else sensors
    row_multiplier=1 if s['query_filter_mode']=='station_and_sensor' else stations
    return (len(per_bucket)*source_partition_multiplicity,
            max(per_bucket.values())*row_multiplier,
            min(per_bucket.values())*row_multiplier)


def evaluate_candidate(s:dict[str,Any],r:dict[str,Any],w:dict[str,Any],p:dict[str,Any]) -> dict[str,Any]:
    cid=s['candidate_id']; fam=s['family']
    if cid!=r['candidate_id'] or cid!=w['candidate_id']:
        raise ValueError('Candidate identity mismatch across Stages 4–6')
    if s['bucket_seconds']!=r['time_bucket_seconds'] or s['bucket_seconds']!=w['bucket_seconds']:
        raise ValueError('Physical bucket width mismatch')
    if s['table']!=r['physical_table'] or s['table']!=w['physical_table']:
        raise ValueError('Physical table identity mismatch')
    if fam not in ('TS0','TS1','TS3') or r['query_id']!='Q_READ_2_WINDOWS' or w['query_id']!='Q_INGEST':
        raise ValueError('Unexpected new operation or family')
    stations=p['station_count']; sensors=p['sensor_types_per_station']
    per_stream=p['horizon_seconds']//p['sample_interval_seconds_per_stream']
    input_rows=stations*sensors*per_stream
    row_bytes=p['logical_record_bytes_assumed']+p['row_storage_overhead_bytes_assumed']
    partitions,max_rows,min_rows=_model_partition_rows(s,p)
    # For TS0 with an unbounded retention horizon this is a finite snapshot only.
    per_partition_proxy=max_rows*row_bytes+p['partition_storage_overhead_bytes_assumed']
    storage_per_replica=input_rows*row_bytes+partitions*p['partition_storage_overhead_bytes_assumed']
    storage_replicated=storage_per_replica*p['replication_factor_assumed']
    start=p['starting_epoch_ms_utc']//1000
    windows=[(start+a,start+b) for a,b in p['two_closed_query_windows_seconds_from_start']]
    bucket_sec=s['bucket_seconds']
    partition_ranges=[len(_buckets_inclusive(a,b,bucket_sec)) if bucket_sec else 1 for a,b in windows]
    fanout=stations if s['query_filter_mode']=='station_and_sensor' else 1
    per_window_requests=[buckets*fanout for buckets in partition_ranges]
    request_count=sum(per_window_requests)
    # Exact events satisfying each inclusive logical window for this uniform scenario.
    interval=p['sample_interval_seconds_per_stream']
    begin=p['starting_epoch_ms_utc']//1000
    def matching_samples(a:int,b:int) -> int:
        lo=max(0,(a-begin+interval-1)//interval)
        hi=min(per_stream-1,(b-begin)//interval)
        return max(0,hi-lo+1)
    rows_in_windows=[matching_samples(a,b)*stations for a,b in windows]
    matching_rows=sum(rows_in_windows)
    partition_limit_rows=p['max_partition_rows_warning']
    partition_limit_bytes=p['max_partition_bytes_warning']
    issues=[]
    if partitions > p['max_total_partition_count_warning']:
        issues.append('TOTAL_PARTITION_COUNT_WARNING')
    if max_rows > partition_limit_rows:issues.append('PARTITION_ROW_WARNING')
    if per_partition_proxy > partition_limit_bytes:issues.append('PARTITION_BYTE_PROXY_WARNING')
    if request_count > p['max_cql_requests_per_logical_read_warning']:
        issues.append('QUERY_CQL_FANOUT_WARNING')
    if matching_rows > p['max_expected_rows_per_logical_read_warning']:
        issues.append('QUERY_MATCHED_ROWS_WARNING')
    if fam=='TS0':issues.append('UNBOUNDED_PARTITION_OVER_TIME')
    if s['query_filter_mode']=='station_and_sensor':issues.append('COMPLETE_STATION_ROSTER_REQUIRED')
    if fam=='TS3':issues.extend(('CANONICAL_SOURCE_REQUIRED','DURABLE_PROJECTION_REPAIR_REQUIRED'))
    issues.extend(('OFFICIAL_KIT_PREDICATE_UNVERIFIED','NO_LIVE_CASSANDRA_VALIDATION'))
    return {
        'candidate_id':cid,'family':fam,'bucket_seconds':bucket_sec,
        'footprint': {
            'assumed_total_source_events': input_rows,'estimated_rows_materialized':input_rows,
            'estimated_physical_partitions':partitions,'max_rows_per_partition_proxy':max_rows,
            'min_rows_per_populated_partition_proxy':min_rows,
            'per_row_bytes_proxy':row_bytes,'max_partition_bytes_proxy':per_partition_proxy,
            'single_replica_logical_bytes_proxy':storage_per_replica,
            'rf_adjusted_logical_bytes_proxy':storage_replicated,
            'storage_is_measured':False,'compression_or_sstable_modeled':False,
        },
        'read': {
            'per_window_partition_groups':partition_ranges,
            'per_window_cql_requests':per_window_requests,
            'total_cql_requests_per_logical_read':request_count,
            'expected_matching_rows_per_window':rows_in_windows,
            'expected_matching_rows_total':matching_rows,
            'expected_cql_selected_payload_bytes_proxy':matching_rows*p['logical_record_bytes_assumed'],
            'expected_final_projected_bytes_proxy':matching_rows*16,
            'partition_fanout_for_station_roster':fanout,
            'estimated_latency_ms':None,'actual_rows_scanned':None,
            'requires_full_roster':s['query_filter_mode']=='station_and_sensor',
        },
        'write': {
            'single_candidate_inserts_per_logical_event':1,
            'single_candidate_insert_statements_for_scenario':input_rows,
            'projected_copy_needs_canonical_source':fam=='TS3',
            'writes_if_combined_with_one_canonical_table':2 if fam=='TS3' else 1,
            'cross_partition_atomicity_proven':False,
            'durable_recovery_proven':False,
            'estimated_iotps':None,
        },
        'feasibility': {
            'status':'CONDITIONAL_ANALYTICAL_ONLY',
            'warnings':issues,
            'hard_rejection':False,
            'true_feasibility_not_proven':True,
        },
    }


def generate(root:Path=ROOT,output_dir:Path|None=None, policy_file:Path|None=None)->dict[str,Any]:
    root=root.resolve()
    policy_file=(policy_file or root/'config/tpcx_stage7_cost_policy.json').resolve()
    out=(output_dir or root/'outputs/tpcx_iot_stage7').resolve()
    if (out==root or out.is_relative_to(root/'inputs') or
        any(out.is_relative_to(root/'outputs'/f'tpcx_iot_stage{x}') for x in ('1_3','4','5','6'))):
        raise ValueError('Stage 7 output overlaps frozen upstream artifacts')
    # Can only read user-supplied policy; must not consult measured results or tune against them.
    policy=read_json(policy_file); validate_policy(policy)
    specs,reads,writes,source_hashes=_verify_stage6(root)
    rr={x['candidate_id']:x for x in reads}
    ww={x['candidate_id']:x for x in writes}
    rows=[evaluate_candidate(s,rr[s['candidate_id']],ww[s['candidate_id']],policy)
          for s in sorted(specs,key=lambda x:x['candidate_id'])]
    if len(rows)!=5 or len(set(x['candidate_id'] for x in rows))!=5:
        raise ValueError('Wrong Stage7 candidate count')
    out.mkdir(parents=True,exist_ok=True)
    write_json(out/'40_scenario_evidence.json',{
        'scenario':policy,'scenario_file_sha256':sha256(policy_file),
        'not_tpcx_official_sf':True,
        'logical_source_events':policy['station_count']*policy['sensor_types_per_station']*(policy['horizon_seconds']//policy['sample_interval_seconds_per_stream']),
        'unverified_source_predicate':'sensor_name_only/raw_read',
        'scenario_is_measured':False,
    })
    write_json(out/'41_physical_footprint_proxies.json',[
        {'candidate_id':x['candidate_id'],'family':x['family'],**x['footprint']} for x in rows])
    write_json(out/'42_read_cost_proxies.json',[
        {'candidate_id':x['candidate_id'],'family':x['family'],**x['read']} for x in rows])
    write_json(out/'43_write_cost_proxies.json',[
        {'candidate_id':x['candidate_id'],'family':x['family'],**x['write']} for x in rows])
    write_json(out/'44_feasibility_diagnostics.json',[
        {'candidate_id':x['candidate_id'],'family':x['family'],**x['feasibility']} for x in rows])
    fieldnames=['candidate_id','family','bucket_seconds','physical_partitions','max_rows_per_partition',
                'max_partition_bytes_proxy','replica_logical_bytes_proxy','total_read_cql_requests',
                'matching_rows_proxy','single_candidate_inserts_per_event','if_canonical_plus_projection_inserts',
                'warnings_count','warning_codes','operational_coverage_proven']
    with (out/'45_candidate_cost_matrix.csv').open('w',newline='',encoding='utf-8') as stream:
        writer=csv.DictWriter(stream,fieldnames=fieldnames);writer.writeheader()
        for x in rows:
            f,r,w,d=x['footprint'],x['read'],x['write'],x['feasibility']
            writer.writerow({
                'candidate_id':x['candidate_id'],'family':x['family'],
                'bucket_seconds':x['bucket_seconds'] if x['bucket_seconds'] is not None else '',
                'physical_partitions':f['estimated_physical_partitions'],
                'max_rows_per_partition':f['max_rows_per_partition_proxy'],
                'max_partition_bytes_proxy':f['max_partition_bytes_proxy'],
                'replica_logical_bytes_proxy':f['rf_adjusted_logical_bytes_proxy'],
                'total_read_cql_requests':r['total_cql_requests_per_logical_read'],
                'matching_rows_proxy':r['expected_matching_rows_total'],
                'single_candidate_inserts_per_event':w['single_candidate_inserts_per_logical_event'],
                'if_canonical_plus_projection_inserts':w['writes_if_combined_with_one_canonical_table'],
                'warnings_count':len(d['warnings']),
                'warning_codes':';'.join(d['warnings']),
                'operational_coverage_proven':'false',
            })
    manifest={
        'stage':7,'engine':'Apache Cassandra',
        'benchmark':'TPCx-IoT 2.1.2 adapted research profile',
        'candidate_count':len(rows),'scenario_is_measured':False,
        'only_prebenchmark_analytical_proxies':True,
        'query_frequency_weights_available':False,
        'score_estimated_workload':None,
        'ranked_candidates':False,'hbench_selected':False,
        'source_coverage':'CONDITIONAL_OFFLINE_ONLY',
        'official_tpcx_iot_kit_verified':False,
        'source_hashes':source_hashes,
        'cost_policy_sha256':sha256(policy_file),
        'upstream_outputs_modified':False,'stage3_reexecuted':False,
        'activation_rules_changed':False,'benchmark_results_used':False,
        'live_cassandra_executed':False,'measured_latency_or_throughput_reported':False,
        'operational_coverage_claimed':False,
        'files':list(OUTPUT_NAMES),
        'next':'Stage8 canonical consolidation and compatible workload-level covers; use only non-measured evidence',
    }
    write_json(out/'46_cost_manifest.json',manifest)
    return manifest
