"""IoT Benchmark Q1-Q12 -> EXISTING SchemaLens TS0-TS8/M1-M5 rules.

This is an explicitly scenario-based evidence adapter, not an IoT Benchmark SQL
parser, candidate generator, database benchmark, or proof of executable plans.

Usage, from this package (base project directory passed explicitly):
 python3 tools/activate_iot_benchmark_ts_only.py \
   --base-root /path/to/SchemaLens_TSDB_RunningExample_SmartRetail

The rule_engine.py and JSON catalogues in --base-root are imported/read without
modification. No C0-C9 rules are executed and no timing evidence is consumed.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_vector(operation: dict, scenario: dict) -> dict:
    """Map documented operation SEMANTICS + declared scenario to rule fields.

    Nothing in this function uses benchmark performance. An assumption is
    explicit in scenario.json, not fabricated from a measured winner.
    """
    return {
        "is_temporal_query": True,
        "returns_raw_observations": operation["returns_raw"],
        "has_time_range": operation["time_range"],
        "series_identity_bound": True,  # Declared experiment scenario
        "metric_count": scenario["query_sensor_num"] if operation["returns_raw"] else 0,
        "has_dimension_equality_anchor": False,
        "latest_only": operation["latest"],
        "time_bucketed": operation["time_bucketed"],
        "aggregate_function_count": int(operation["aggregate"]),
        "non_time_group_dimension_count": 0,
        "series_discovery": False,
        "model_segment_requested": False,
        "retention_days": None,
        "append_oriented": scenario["append_oriented"],
        "hot_series": False,  # false indicates no positive hot-series evidence
        "late_arrival_allowed": None,
    }


def compute(base: Path, scenario: dict, operations: list[dict]):
    """Invoke exactly the reusable unmodified rule engine from the base ZIP."""
    if str(base) not in sys.path:
        sys.path.insert(0, str(base))
    from methodology.common.rule_engine import activate, evaluate_rule

    ts_rules_path = base / 'config/tsdb_activation_rules.json'
    m_rules_path = base / 'config/temporal_modifier_rules.json'
    ts_catalog_path = base / 'config/tsdb_family_catalog.json'
    rules = read_json(ts_rules_path)
    modifiers = read_json(m_rules_path)
    catalog = read_json(ts_catalog_path)
    assert {x['family'] for x in rules['rules']} == {x['family'] for x in catalog['families']}
    assert {x['family'] for x in rules['rules']} == {f'TS{i}' for i in range(9)}
    assert {x['family'] for x in modifiers['rules']} == {f'M{i}' for i in range(1, 6)}
    if scenario['dialect'] not in ('tree', 'table'):
        raise ValueError('Invalid dialect')
    if scenario['query_device_num'] <= 0 or scenario['query_sensor_num'] <= 0:
        raise ValueError('Query device/sensor counts must be positive')
    if scenario['benchmark_used_to_activate']:
        raise ValueError('Post-benchmark feedback prohibited')

    output = []
    for operation in operations:
        if operation['id'] == 'Q12' and scenario['dialect'] != 'table':
            continue
        v = build_vector(operation, scenario)
        activated, trace = activate(v, rules)
        chosen_mods = []
        mod_trace = []
        for rule in modifiers['rules']:
            outcome = evaluate_rule(v, rule, activated + chosen_mods)
            if outcome.passed:
                chosen_mods.append(outcome.family)
            mod_trace.append(asdict(outcome))
        output.append({
            'query_id': operation['id'], 'operation_name': operation['name'],
            'dialect': scenario['dialect'], 'features': v,
            'ts_families': activated, 'modifiers': chosen_mods,
            'ts_rule_trace': trace, 'modifier_trace': mod_trace,
            'coverage_status': 'NOT_VALIDATED',
            'benchmark_executed': False,
            'semantic_assumptions': scenario['assumptions']
        })
    return output, {
        'ts_rules_sha256': sha256(ts_rules_path),
        'modifier_rules_sha256': sha256(m_rules_path),
        'ts_catalog_sha256': sha256(ts_catalog_path),
        'base_rule_engine_sha256': sha256(base / 'methodology/common/rule_engine.py'),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base-root', type=Path, required=True,
                    help='Root of original SmartRetail V2 package, not a Stage 4-9 patch')
    ap.add_argument('--scenario', type=Path, default=PACKAGE_ROOT/'inputs/scenario.json')
    ap.add_argument('--operations', type=Path, default=PACKAGE_ROOT/'inputs/operations.json')
    ap.add_argument('--output-dir', type=Path, default=PACKAGE_ROOT/'outputs')
    args = ap.parse_args()
    scenario, operations = read_json(args.scenario), read_json(args.operations)
    rows, hashes = compute(args.base_root.resolve(), scenario, operations)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with (args.output_dir/'iot_benchmark_ts_only_activation.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['query_id', 'operation_name', 'dialect',
                                               *[f'TS{i}' for i in range(9)],
                                               *[f'M{i}' for i in range(1,6)],
                                               'ts_families','modifiers','coverage_status'])
        writer.writeheader()
        for r in rows:
            writer.writerow({'query_id':r['query_id'],'operation_name':r['operation_name'],
                             'dialect':r['dialect'],
                             **{f'TS{i}':f'TS{i}' in r['ts_families'] for i in range(9)},
                             **{f'M{i}':f'M{i}' in r['modifiers'] for i in range(1,6)},
                             'ts_families':' | '.join(r['ts_families']),
                             'modifiers':' | '.join(r['modifiers']),
                             'coverage_status':r['coverage_status']})
    (args.output_dir/'iot_benchmark_ts_only_trace.json').write_text(json.dumps(rows, indent=2, ensure_ascii=False)+'\n', encoding='utf-8')
    (args.output_dir/'iot_benchmark_activation_manifest.json').write_text(json.dumps({
        'status':'EXPLORATORY_TS_ONLY_ACTIVATION_NO_CQL_NO_BENCHMARK',
        'scenario':scenario, 'query_count':len(rows), 'rules_hashes':hashes,
        'input_sha256':{'scenario':sha256(args.scenario), 'operations':sha256(args.operations)},
        'allowed_catalogue':'TS0-TS8 + M1-M5 only',
        'cassandra_C_families_evaluated':False,
        'stage4_9_executed':False,
        'real_iot_benchmark_sql_parsed':False,
        'latency_or_storage_measured':False,
        'physical_candidate_coverage_verified':False
    },indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    for row in rows:
        print(f"{row['query_id']:>3} {row['operation_name']:<52} TS={','.join(row['ts_families']) or '-'}  M={','.join(row['modifiers']) or '-'}")
    print('Outputs:',args.output_dir)

if __name__ == '__main__':
    main()
