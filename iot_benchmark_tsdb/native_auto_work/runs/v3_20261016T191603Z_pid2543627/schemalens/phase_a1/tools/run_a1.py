"""Run phase A1 without Docker, Cassandra or third-party packages."""
import argparse
import csv
import json
import sys
from pathlib import Path

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from core.bridge import read_native, make_queries, make_cassandra_specs, coverage_decisions, json_write, digest, read_properties, _result
from tools.activate_iot_benchmark_ts_only import compute

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--native-data',type=Path,default=HERE/'fixtures/native')
    p.add_argument('--output',type=Path,default=HERE/'outputs')
    p.add_argument('--max-rows',type=int,default=100_000)
    p.add_argument('--official-query-objects',type=Path,default=None,help='Actual Java parameter-object JSONL; uses one sample per Q class')
    p.add_argument('--properties',type=Path,default=HERE/'inputs/iot_benchmark_pilot.properties',help='Same generator configuration used for data and query capture')
    args=p.parse_args()
    scenario=json.loads((HERE/'inputs/scenario.json').read_text(encoding='utf-8'))
    operations=json.loads((HERE/'inputs/operations.json').read_text(encoding='utf-8'))
    config=read_properties(args.properties)
    if config['IoTDB_DIALECT_MODE']!=scenario['dialect']:
        raise ValueError('Dialect mismatch between pilot and activation scenario')
    if config['BENCHMARK_WORK_MODE']!='generateDataMode':
        raise ValueError('Initial phase A1 must use generated data mode')
    if config['TIMESTAMP_PRECISION']!='ms':
        raise ValueError('Phase A1 currently supports only ms timestamp precision')
    if config['INSERT_DATATYPE_PROPORTION']!='0:0:0:0:1:0:0:0:0:0:0':
        raise ValueError('Phase A1 demands only DOUBLE fields')
    data=read_native(args.native_data,max_rows=args.max_rows)
    if int(config['SENSOR_NUMBER'])!=len(data['sensors']):
        raise ValueError('Sensor number must match native file')
    if int(config['DEVICE_NUMBER'])!=len(data['devices']):
        raise ValueError('Device number must match native file')
    for prop,skey in [('QUERY_DEVICE_NUM','query_device_num'),('QUERY_SENSOR_NUM','query_sensor_num'),('GROUP_BY_TIME_UNIT','group_by_time_unit_ms'),('QUERY_LOWER_VALUE','query_lower_value'),('QUERY_AGGREGATE_FUN','query_aggregate_fun')]:
        if str(scenario[skey])!=config[prop]:raise ValueError(f'Property/scenario mismatch: {prop}')
    rows,rules_hashes=compute(HERE/'vendor/tsdb_base',scenario,operations)
    if args.official_query_objects:
        from tools.official_query_capture import validate,representative_queries
        if config.get('IS_SENSOR_TS_ALIGNMENT','true').lower()!='true':
            raise ValueError('Native bridge currently requires IS_SENSOR_TS_ALIGNMENT=true')
        if config.get('NULL_RATIO','0') not in ('0','0.0'):
            raise ValueError('Native parameter-object bridge does not yet support null SQL semantics: NULL_RATIO must be zero')
        events,_=validate(args.official_query_objects,[f'Q{i}' for i in range(1,13)])
        queries=representative_queries(events,data,scenario,operations,args.official_query_objects)
        for q in queries:
            if len(q['sensors'])!=int(config['QUERY_SENSOR_NUM']):
                raise ValueError('Native Q selected a different sensor count than QUERY_SENSOR_NUM: '+q['query_id'])
            if q['query_id'] in {'Q4','Q5','Q6','Q7','Q11'} and q['aggregation']!=scenario['query_aggregate_fun']:
                raise ValueError('Native Q aggregation function differs from configured scenario: '+q['query_id'])
            if q['query_id'] in {'Q7','Q11'} and q['group_by_ms']!=int(config['GROUP_BY_TIME_UNIT']):
                raise ValueError('Native Q group by width differs from config: '+q['query_id'])
    else:
        queries=make_queries(data,scenario,operations)
    specs=make_cassandra_specs(data['sensors'],scenario)
    coverage=coverage_decisions(rows,queries,specs)
    out=args.output; out.mkdir(parents=True,exist_ok=True)
    json_write(out/'activation_ts_only.json',rows)
    json_write(out/'query_instances_pilot.json',queries)
    json_write(out/'cassandra_ddl_blueprints.json',specs)
    json_write(out/'coverage_gates.json',coverage)
    json_write(out/'reference_answers_pilot.json',{q['query_id']:_result(data['records'],q) for q in queries})
    with (out/'normalized_double_readings.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=['device_id','sensor_name','timestamp','value_double']); w.writeheader();w.writerows(data['records'])
    (out/'cassandra_blueprints.cql').write_text('-- Blueprints only: do not treat this as a benchmark-ready deployment.\n'+'\n\n'.join(x['ddl'] for x in specs)+'\n',encoding='utf-8')
    with (out/'activation_table.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=['query_id','operation_name','ts_families','modifiers','coverage'])
        w.writeheader()
        for row in rows:w.writerow({'query_id':row['query_id'],'operation_name':row['operation_name'],'ts_families':'|'.join(row['ts_families']), 'modifiers':'|'.join(row['modifiers']),'coverage':'NOT_VALIDATED'})
    manifest={'status':'A1_ANALYTICAL_AND_REFERENCE_ONLY','database_target':'Cassandra',
              'dataset_origin':'native format: fixture unless --native-data points to genuine IoT Benchmark generation',
              'devices':data['devices'],'sensors':data['sensors'],'metric_cells':data['count'],
              'batch_headers':data['batch_headers'],'queries':len(queries),'physical_blueprints':len(specs),
              'activation_occurrences':sum(len(x['ts_families']) for x in rows),
              'schema_source_sha256':data['files_sha256'],
              'rules_sha256':rules_hashes,
              'config_sha256':digest(args.properties),
              'parameter_capture_sha256':digest(args.official_query_objects) if args.official_query_objects else None,
              'java_generator_parameter_objects_sampled':bool(args.official_query_objects),
              'inputs_sha256':{p.name:digest(p) for p in (HERE/'inputs').glob('*.json')},
              'native_query_seed_replayed':False,'iot_benchmark_sql_verified':False,
              'C0_C9_used':False,'physical_coverage_verified':False,'HBench_selected':False,
              'Cassandra_live_execution':False,'benchmark_measured':False,
              'stage4_9_legacy_integrated':False,
              'blocking_issues':['Rebuild Stage4 candidate generator for generic IoT sensors (CPU-only ten usage_* restriction)',
                                 'Implement general Stage5 QueryPlans incl Q12 client set operations',
                                 'Implement Stage6 exact WritePlans incl TS4/latest and TS5 rollup','Adapt Stage7+8 cost model and coverage to generic metrics and Q1-Q12',
                                 'Stage9 Cassandra driver currently only reads native TSBS CPU pseudoCSV',
                                 'Replay actual benchmark query parameter sequence and verify SQL semantics before formal evaluation']}
    json_write(out/'manifest.json',manifest)
    print(f"A1 completed: {data['count']} metric cells, {len(queries)} query classes, {len(specs)} Cassandra DDL blueprints")
    print('No HBench, no measured benchmark, all coverage NOT_VALIDATED')
    if args.official_query_objects:print('JAVA QUERY-OBJECT SAMPLE; not native SQL or frequency/sequence replay')
    for x in rows:print(f"{x['query_id']:3s}: {','.join(x['ts_families']):14s} modifiers={','.join(x['modifiers'])}")
    print(out.resolve())

if __name__=='__main__':main()
