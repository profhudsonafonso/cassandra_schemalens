# SchemaLens TPCx-IoT preliminary Steps 1-3: EER -> M -> activation.
# Transparent OFFLINE prototype, not official TPC kit or benchmark.
# No measured latency/throughput; English code comments for supervisors.
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from collections import Counter, deque
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from methodology.common.rule_engine import activate, evaluate_rule
from methodology.smart_retail.sql_evidence import extract_sql_evidence
from methodology.smart_retail.evidence_builder import _derived_temporal_features

ROOT = Path(__file__).resolve().parents[2]


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).lower() if isinstance(value, bool) else value


def save_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = columns or (list(rows[0]) if rows else [])
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_value(row.get(key)) for key in columns})


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def conceptual_schema(events_per_series_day: float | None, retention_days: int | None, allow_late_arrival: bool) -> dict[str, Any]:
    # The EER is a research abstraction. The official TPC specification gives a flat record.
    return {
        'dataset': 'TPCx-IoT 2.1.2',
        'status': 'RESEARCH_EER_ABSTRACTION_NOT_OFFICIAL_TPC_SCHEMA',
        'source': 'https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPCX-IOT_v2.1.2.pdf',
        'specification': {'sensor_types': 200, 'record_approximate_bytes': 1024,
                          'driver_id_represents': 'electrical power station',
                          'reading_query': 'one randomly selected sensor, two 5-second time intervals'},
        'entities': {
            'PowerStation': {'identity': ['driver_system_id'], 'attributes': ['driver_system_id']},
            'SensorStream': {'identity': ['driver_system_id', 'sensor_name'],
                             'attributes': ['driver_system_id', 'sensor_name']},
            'Reading': {'identity': ['driver_system_id', 'sensor_name', 'event_ts'],
                        'attributes': ['driver_system_id', 'sensor_name', 'event_ts', 'sensor_reading', 'padding'],
                        'temporal': {
                            'series_identity': ['driver_system_id', 'sensor_name'],
                            'time_attribute': 'event_ts',
                            'metrics': ['sensor_reading'],
                            'dimensions': ['driver_system_id', 'sensor_name'],
                            'retention_days': retention_days,
                            'estimated_events_per_day': events_per_series_day,
                            'append_oriented': True,
                            'late_arrival_allowed': allow_late_arrival,
                        }},
        },
        'relationships': [
            {'name': 'station_has_stream', 'left': 'PowerStation', 'right': 'SensorStream',
             'cardinality': '1:N', 'semantics': 'containment_like', 'source': 'research_assumption'},
            {'name': 'stream_has_reading', 'left': 'SensorStream', 'right': 'Reading',
             'cardinality': '1:N', 'semantics': 'time_series_observation', 'source': 'research_assumption'},
        ],
        'known_unknowns': {
            'sensor_selection_predicate_in_kit': 'MUST_VERIFY_AGAINST_KIT_BINDING',
            'per_series_rate': 'NOT_ESTABLISHED_BY_SPECIFICATION' if events_per_series_day is None else 'EXPLICIT_USER_SCENARIO',
            'retention': 'NOT_ESTABLISHED' if retention_days is None else 'EXPLICIT_USER_SCENARIO',
            'late_arrival': 'NOT_ESTABLISHED' if not allow_late_arrival else 'EXPLICIT_USER_SCENARIO',
            'update_exposure_frequency': 'UNSPECIFIED_NO_WORKLOAD_FREQUENCIES',
        },
    }


def workload(read_access_mode: str, read_semantics: str = 'raw_read') -> list[dict[str, Any]]:
    if read_access_mode not in ('sensor-name-only','station-and-sensor'):
        raise ValueError('Unsupported read access mode')
    if read_semantics not in ('raw_read', 'aggregate_templates'):
        raise ValueError('Unsupported read semantics')
    # Current specification says 'randomly selected sensor'. Exact fields need KIT verification.
    station_predicate = 'r.driver_system_id = :station_id AND ' if read_access_mode == 'station-and-sensor' else ''
    # BETWEEN is inclusive in this logical example, matching the closed notation in the spec.
    # The official KIT controls actual window generation and inclusivity for final execution.
    query_sql = f'''SELECT r.event_ts, r.sensor_reading
FROM Reading r
WHERE {station_predicate}r.sensor_name = :sensor_name
  AND ((r.event_ts BETWEEN :ti1_start AND :ti1_end)
    OR (r.event_ts BETWEEN :ti2_start AND :ti2_end));'''
    ingest = {
        'query_id': 'Q_INGEST', 'operation': 'INSERT', 'query_name': 'Ingest sensor reading',
        'source': 'TPCX_IOT_SPEC_SECTION_2_2', 'query_class': 'insert',
        'touched_entities': ['Reading'], 'logical_sql':
        'INSERT INTO Reading (driver_system_id, sensor_name, event_ts, sensor_reading, padding)\n'
        'VALUES (:station_id, :sensor_name, :event_ts, :value, :padding);',
        'official_workload_operation': 'continuous_sensor_ingestion',
        'predicate_confidence': 'SOURCE_DEFINED',
        'frequency_status': 'NOT_SPECIFIED_AS_QUERY_MIX_WEIGHT'
    }
    raw_read = {
        'query_id': 'Q_READ_2_WINDOWS', 'operation': 'SELECT',
        'query_name': 'Read recent and historical windows',
        'source': 'TPCX_IOT_SPEC_SECTION_2_2', 'query_class': 'temporal_range',
        'touched_entities': ['SensorStream', 'Reading'], 'logical_sql': query_sql,
        'official_workload_operation': 'one_query_two_intervals',
        'time_windows_seconds': [5,5], 'historical_lookback_seconds': 1800,
        'predicate_confidence': 'ASSUMPTION_TO_VERIFY_IN_KIT',
        'frequency_status': 'NOT_SPECIFIED_AS_QUERY_MIX_WEIGHT'
    }
    if read_semantics == 'raw_read':
        return [ingest, raw_read]
    # A secondary academic analysis reports four aggregate templates. They are
    # deliberately optional and NEVER presented as verified against the 2.1.2 KIT.
    # CASE WHEN yields one aggregate for EACH of the two windows, not for their union.
    aggregates = []
    for op in ('MAX','MIN','AVG','COUNT'):
        query = dict(raw_read)
        query['query_id'] = 'Q_READ_' + op
        query['query_name'] = f'Compare {op} over two 5-second windows'
        query['source'] = 'SECONDARY_ACADEMIC_TPCX_STUDY_NOT_KIT_VERIFIED'
        query['predicate_confidence'] = 'SECONDARY_ACADEMIC_INTERPRETATION_NEEDS_KIT'
        query['query_class'] = 'temporal_window_aggregate'
        query['aggregate_operation'] = op
        query['logical_sql'] = (
            f'SELECT {op}(CASE WHEN r.event_ts BETWEEN :ti1_start AND :ti1_end '
            f'THEN r.sensor_reading END) AS recent_stat,\n'
            f'       {op}(CASE WHEN r.event_ts BETWEEN :ti2_start AND :ti2_end '
            f'THEN r.sensor_reading END) AS historical_stat\n'
            f'FROM Reading r\nWHERE {station_predicate}r.sensor_name = :sensor_name\n'
            f'  AND ((r.event_ts BETWEEN :ti1_start AND :ti1_end)\n'
            f'    OR (r.event_ts BETWEEN :ti2_start AND :ti2_end));'
        )
        aggregates.append(query)
    return [ingest, *aggregates]



def shortest_distance(a: str, b: str, schema: dict[str, Any]) -> int:
    graph: dict[str, list[str]] = {ent: [] for ent in schema['entities']}
    for rel in schema['relationships']:
        graph[rel['left']].append(rel['right'])
        graph[rel['right']].append(rel['left'])
    seen = {a}
    queue = deque([(a,0)])
    while queue:
        node, d = queue.popleft()
        if node == b: return d
        for neighbor in graph[node]:
            if neighbor not in seen:
                seen.add(neighbor)
                queue.append((neighbor,d+1))
    raise ValueError(f'Disconnected EER for {a} -> {b}')


def root_selection(schema: dict[str, Any], query: dict[str, Any],
                   read_access_mode: str) -> tuple[str, str, dict[str, Any]]:
    """Follow the semantic-first, lexicographic fallback of DKE Algorithm 2.

    The access-anchor extractor is currently a controlled TPCx-IoT adapter.
    It must be generalized before claiming implementation for arbitrary EERs.
    """
    touched = query['touched_entities']
    if len(touched) == 1:
        only = touched[0]
        return only, 'single entity in query fragment', {}
    if query['operation'] == 'INSERT':
        candidates = ['Reading'] if 'Reading' in touched else []
    elif read_access_mode == 'station-and-sensor':
        candidates = ['SensorStream'] if 'SensorStream' in touched else []
    else:
        candidates = ['Reading'] if 'Reading' in touched else []
    if len(candidates) == 1:
        rationale = ('fully bound sensor stream' if candidates[0] == 'SensorStream'
                     else 'sensor-name filter across potentially multiple stations')
        return candidates[0], rationale, {}
    eligible = candidates if candidates else touched
    scores = {}
    for entity in eligible:
        distances = [shortest_distance(entity, e, schema) for e in touched if e != entity]
        direct_links = sum(d == 1 for d in distances)
        scores[entity] = (sum(distances) / len(distances), max(distances), -direct_links)
    ranked = sorted(eligible, key=lambda entity: (scores[entity], entity))
    return ranked[0], 'semantic tie / graph-structural fallback', {k:list(v) for k,v in scores.items()}


def analytical_record(schema: dict[str, Any], query: dict[str, Any], read_access_mode: str) -> dict[str, Any]:
    touched = query['touched_entities']
    root, rationale, root_scores = root_selection(schema,query,read_access_mode)
    edges = [rel for rel in schema['relationships'] if rel['left'] in touched and rel['right'] in touched]
    depth = max(shortest_distance(root,e,schema) for e in touched)
    rc = len(edges)
    re_at_1 = rc - sum(int(shortest_distance(root,rel['left'],schema) <= 1 and
                           shortest_distance(root,rel['right'],schema) <= 1) for rel in edges)
    return {
        'query_id': query['query_id'], 'selected_root': root,
        'root_rationale': rationale, 'root_fallback_scores': root_scores,
        'query_class': query['query_class'], 'touched_entities': touched,
        'touched_relationships': [r['name'] for r in edges],
        'schema_rc': rc, 'selected_document_depth': depth,
        'selected_re_at_d1': re_at_1,
        'dominant_semantics': 'temporal_observation',
        'sharedness_evidence': 'EER_OWNERSHIP_ONLY_NO_INSTANCE_OBSERVATIONS',
        'update_exposure_evidence': 'INGESTION_EXISTS_BUT_QUERY_FREQUENCIES_UNSPECIFIED',
        'time_series_extension': True,
    }


def temporal_evidence(schema: dict[str, Any], query: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    if query['operation'] == 'SELECT':
        sql_ev = extract_sql_evidence(query['logical_sql'])
        if sql_ev.entities != ['Reading']:
            raise ValueError('Unsupported query structure for evidence extractor')
        values = _derived_temporal_features(sql_ev, schema, 'TEMPORAL', policy)
        if not values['has_time_range']:
            raise ValueError('Time range was not detected')
        if len(query['time_windows_seconds']) != 2:
            raise ValueError('Official 2-window read must remain a single query')
    else:
        # Original feature helper parses SELECTs only; ingestion requires an explicit
        # operation-aware adapter rather than pretending INSERT is a SELECT.
        sql_ev = None
        series = schema['entities']['Reading']['temporal']
        vol = series['estimated_events_per_day']
        values = {
            'is_temporal_query': True, 'temporal_source_entity': 'Reading',
            'time_attribute': 'event_ts', 'series_identity_fields': list(series['series_identity']),
            'series_identity_bound': True, 'has_time_range': False,
            'time_ordered': False, 'latest_only': False,
            'metric_fields_projected': [], 'metric_count': 0,
            'dimension_equality_fields': [], 'has_dimension_equality_anchor': False,
            'returns_raw_observations': False, 'time_bucketed': False,
            'non_time_group_dimensions': [], 'non_time_group_dimension_count': 0,
            'series_discovery': False, 'model_segment_requested': False,
            'retention_days': series['retention_days'],
            'estimated_events_per_day': vol,
            'append_oriented': True, 'late_arrival_allowed': bool(series['late_arrival_allowed']),
            'hot_series': vol is not None and float(vol) >= float(policy['hot_series_events_per_day_threshold']),
        }
    values.update({
        'operation': query['operation'], 'aggregate_function_count': (sql_ev.aggregate_function_count if sql_ev else 0),
        'temporal_window_count': len(query.get('time_windows_seconds',[])),
        'temporal_window_lengths_seconds': query.get('time_windows_seconds',[]),
        'historical_lookback_seconds': query.get('historical_lookback_seconds'),
        'predicate_confidence': query['predicate_confidence'],
    })
    return values


def run(root: Path, output: Path, read_access_mode: str,
        events_per_series_day: float | None = None, retention_days: int | None = None,
        allow_late_arrival: bool = False, read_semantics: str = 'raw_read') -> dict[str, Any]:
    if events_per_series_day is not None and events_per_series_day <= 0:
        raise ValueError('events_per_series_day must be positive')
    if retention_days is not None and retention_days <= 0:
        raise ValueError('retention_days must be positive')
    if not output.resolve().is_relative_to(root.resolve()):
        raise ValueError('Output must be inside project root to support complete snapshots')
    policy=read_json(root/'config/temporal_policy.json')
    ts_rules=read_json(root/'config/tsdb_activation_rules.json')
    mod_rules=read_json(root/'config/temporal_modifier_rules.json')
    catalogue=read_json(root/'config/tsdb_family_catalog.json')
    families=[r['family'] for r in ts_rules['rules']]
    if families != [family['family'] for family in catalogue['families']]:
        raise ValueError('Temporal rule/catalogue mismatch')
    schema=conceptual_schema(events_per_series_day,retention_days,allow_late_arrival)
    save_json(root/'inputs/schema/tpcx_iot_eer.json', schema)
    queries=workload(read_access_mode,read_semantics)
    manifest=[]
    analytical=[]
    trace=[]
    evidence_matrix=[]
    activation_matrix=[]
    modifier_matrix=[]
    for query in queries:
        filename=f"{query['query_id']}.sql"
        (root/'inputs/workload/sql').mkdir(parents=True,exist_ok=True)
        (root/'inputs/workload/sql'/filename).write_text(query['logical_sql']+'\n',encoding='utf-8')
        manifest.append({k:query.get(k) for k in ('query_id','query_name','operation','query_class','official_workload_operation','predicate_confidence','frequency_status')}
                        | {'sql_file':filename})
        a=analytical_record(schema,query,read_access_mode)
        analytical.append(a)
        ev=temporal_evidence(schema,query,policy)
        ev.update({'schema_rc':a['schema_rc'],'selected_document_depth':a['selected_document_depth'],
                   'selected_re':a['selected_re_at_d1'],'selected_root':a['selected_root'],
                   'sharedness_status':a['sharedness_evidence'],
                   'update_exposure_status':a['update_exposure_evidence']})
        evidence_matrix.append({'query_id':query['query_id'],**ev})
        active_ts,ts_trace=activate(ev,ts_rules)
        mods=[];mod_trace=[]
        for rule in mod_rules['rules']:
            result=evaluate_rule(ev,rule,active_ts+mods)
            if result.passed:mods.append(result.family)
            mod_trace.append(asdict(result))
        activation_matrix.append({'query_id':query['query_id'],**{f:(f in active_ts) for f in families},
                                  'active_families':active_ts})
        modifier_matrix.append({'query_id':query['query_id'],**{r['family']:(r['family'] in mods) for r in mod_rules['rules']},
                                'active_modifiers':mods})
        trace.append({'query_id':query['query_id'],'analytical_row':a,'evidence':ev,
                      'active_ts_families':active_ts,'active_modifiers':mods,
                      'ts_rule_trace':ts_trace,'modifier_rule_trace':mod_trace,
                      'provenance':{'source':'TPCx-IoT v2.1.2 Section 2.2',
                                    'logical_sql':filename,'assumptions':['Sensor query binding must be verified in official kit'],
                                    'benchmark_results_used':False}})
    save_csv(root/'inputs/workload/query_manifest.csv',manifest)
    output.mkdir(parents=True,exist_ok=True)
    save_csv(output/'01_analytical_matrix.csv',analytical)
    save_csv(output/'02_temporal_evidence.csv',evidence_matrix)
    save_csv(output/'03_ts_activation_matrix.csv',activation_matrix,
             ['query_id',*families,'active_families'])
    save_csv(output/'04_modifier_matrix.csv',modifier_matrix,
             ['query_id',*[r['family'] for r in mod_rules['rules']],'active_modifiers'])
    save_json(output/'05_explanation_trace.json',trace)
    files=[root/'inputs/schema/tpcx_iot_eer.json',root/'inputs/workload/query_manifest.csv',
           *[root/'inputs/workload/sql'/r['sql_file'] for r in manifest],
           *[root/'config'/p for p in ('tsdb_activation_rules.json','tsdb_family_catalog.json',
                                       'temporal_modifier_rules.json','temporal_policy.json')]]
    # Freeze inputs and SHA-256 provenance for each scenario.
    if output.resolve().is_relative_to((root/'inputs').resolve()):
        raise ValueError('Cannot write outputs into inputs directory')
    save_csv(output/'00_inputs_sha256.csv',[
        {'file':str(p.relative_to(root)),'sha256':sha256(p),'bytes':p.stat().st_size} for p in files])
    for file in files:
        copy=output/'inputs_snapshot'/file.relative_to(root)
        copy.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(file,copy)
    summary={
        'status':'OFFLINE_ANALYTICAL_ONLY_STAGES_1_TO_3',
        'benchmark':'TPCx-IoT','specification_version':'2.1.2',
        'read_access_mode':read_access_mode,
        'read_semantics':read_semantics,
        'read_access_assumption':'NEEDS_OFFICIAL_KIT_VALIDATION',
        'schema_lens_reference':'DKE From Workloads to Explainable Document Schema Design Alternatives',
        'queries':len(queries),'temporal_families':families,
        'activated_counts':dict(Counter(f for t in trace for f in t['active_ts_families'])),
        'modifier_counts':dict(Counter(f for t in trace for f in t['active_modifiers'])),
        'note':'TS0-TS8 and M1-M5 rules reused unchanged from supplied TSBS package.',
        'uncertainties':['Official kit not included; precise sensor query key and aggregate query templates still unverified',
                         'No MongoDB binding or physical schema produced',
                         'M3 is a technology-dependent compaction consideration, not a MongoDB DDL directive',
                         'No ranking, HBench, measured latency or IoTps',
                         'No workload operation frequencies are assumed'],
        'artifacts':sorted(str(p.relative_to(output)) for p in output.glob('*') if p.is_file()),
    }
    save_json(output/'06_run_manifest.json',summary)
    return summary


def cli() -> None:
    parser=argparse.ArgumentParser(description='SchemaLens preliminary TPCx-IoT TS-only Stage 1-3')
    parser.add_argument('--read-access-mode', choices=['sensor-name-only','station-and-sensor'],
                        default='sensor-name-only',help='Pre-registration assumption, pending kit verification')
    parser.add_argument('--read-semantics',choices=['raw_read','aggregate_templates'],default='raw_read',
                        help='Aggregate templates from secondary research; kit semantics unverified')
    parser.add_argument('--events-per-series-day',type=float,default=None,
                        help='Explicit scenario assumption; no rate derived from specification')
    parser.add_argument('--retention-days',type=int,default=None)
    parser.add_argument('--allow-late-arrival',action='store_true')
    parser.add_argument('--output-dir',default='outputs/tpcx_iot_stage1_3',type=Path)
    args=parser.parse_args()
    out=args.output_dir if args.output_dir.is_absolute() else ROOT/args.output_dir
    result=run(ROOT,out,args.read_access_mode,args.events_per_series_day,
               args.retention_days,args.allow_late_arrival,args.read_semantics)
    print(json.dumps(result,indent=2,ensure_ascii=False))

if __name__=='__main__':
    cli()
