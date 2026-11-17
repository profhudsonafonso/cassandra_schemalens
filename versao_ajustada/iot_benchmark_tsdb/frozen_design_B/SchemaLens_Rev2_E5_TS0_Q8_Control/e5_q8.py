#!/usr/bin/env python3
"""E5: read-only paired control for TS0 Q8, separate from frozen SchemaLens Rev2.

Compare the *actual unchanged Stage9 executor* (full scan) with an alternative
plan that queries LIMIT 1 for each sensor partition. No Cassandra writes, no
Stage7/8 changes, and no retrospective alteration of the confirmatory benchmark.
This is client end-to-end latency, not server-side CQL execution time.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import re
import statistics
import sys
import time

SQL_FULL = ('SELECT timestamp_ms,value_double FROM ts0_iot_raw '
            'WHERE device_id=%s AND sensor_name=%s')
SQL_LATEST = SQL_FULL + ' LIMIT 1'
SELECT_TABLE = 'SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s'
ALLOWED_SQL = {SQL_FULL, SQL_LATEST}
EXPERIMENT_FILES = ('manifest.json', 'raw_latencies.csv', 'query_p95.csv', 'orders.csv',
                    'aggregated.csv', 'load.csv')


def ensure(cond: bool, message: str) -> None:
    if not cond:
        raise ValueError(message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, records: list[dict], columns: list[str]) -> None:
    with path.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)


def percentile_nearest_rank(xs: list[float], percentile: float) -> float:
    ensure(bool(xs), 'Percentile requires measurements')
    vals = sorted(float(v) for v in xs)
    return vals[min(len(vals)-1, max(0, math.ceil(percentile/100*len(vals))-1))]


def balanced_orders(pairs: int, seed: int) -> list[str]:
    ensure(pairs >= 2 and pairs % 2 == 0, 'Require an even number of measurement pairs >= 2')
    orders = ['AB']*(pairs//2) + ['BA']*(pairs//2)
    random.Random(seed).shuffle(orders)
    return orders


class CountingSession:
    """Allow ONLY immutable, parameter-bound TS0 Q8 SELECT statements.

    execute_calls counts Python Session.execute invocations, NOT coordinator or
    paging network roundtrips. iterated_rows counts rows consumed by Python.
    """
    def __init__(self, session, allowed_pairs: set[tuple[str, str]]):
        self.session = session
        self.allowed_pairs = allowed_pairs
        self.execute_calls = 0
        self.iterated_rows = 0
        self.sql_counts = Counter()

    def execute(self, statement, parameters=None, *args, **kwargs):
        ensure(isinstance(statement, str) and statement in ALLOWED_SQL,
               'Only the two preapproved TS0 SELECT statements are accepted')
        ensure(not args and not kwargs, 'Unexpected driver execute options')
        ensure(isinstance(parameters, (tuple, list)) and len(parameters) == 2,
               'Exactly two bound query parameters are required')
        pair = tuple(parameters)
        ensure(pair in self.allowed_pairs, 'Query scope differs from frozen Q8 device/sensor selection')
        self.execute_calls += 1
        self.sql_counts['LIMIT1' if statement == SQL_LATEST else 'FULL'] += 1
        inner = self.session.execute(statement, parameters)
        owner = self

        class CountedRows:
            def __iter__(self):
                for row in inner:
                    owner.iterated_rows += 1
                    yield row

        return CountedRows()


def optimized_q8(session, q: dict, logical_result_fn):
    """Read one latest row per TS0 sensor partition, preserving Q8 result shape.

    The TS0 loader stores only observed non-null sensor readings. For a table
    containing an unexpected null-valued row, fall back to a full partition
    scan for that specific (device,sensor) and ignore null values. This
    fallback is explicitly counted and should be absent in the frozen run.
    """
    ensure(q.get('query_id') == 'Q8', 'This control only supports Q8')
    ensure(q.get('value_lower') is None, 'Unexpected Q8 value predicate')
    rows = []
    null_fallbacks = 0
    for device in q['devices']:
        for sensor in q['sensors']:
            first = next(iter(session.execute(SQL_LATEST, (device, sensor))), None)
            if first is None:
                continue  # No observations for this sensor partition.
            if first.value_double is None:
                null_fallbacks += 1
                for row in session.execute(SQL_FULL, (device, sensor)):
                    if row.value_double is not None:
                        rows.append({'device_id': device, 'sensor_name': sensor,
                                     'timestamp': int(row.timestamp_ms),
                                     'value_double': float(row.value_double)})
                continue
            rows.append({'device_id': device, 'sensor_name': sensor,
                         'timestamp': int(first.timestamp_ms),
                         'value_double': float(first.value_double)})
    return logical_result_fn(rows, q), null_fallbacks


def check_paths(exp: Path, results: Path, addon: Path, out: Path):
    for folder in (exp, results, addon):
        ensure(not out.is_relative_to(folder),
               'Output must not be nested in frozen inputs or addon: ' + str(folder))
    ensure(not out.exists(), 'Refusing to overwrite existing E5 output: ' + str(out))
    ensure(results.is_relative_to(exp), 'Confirmatory results must live inside frozen experiment')
    ensure((addon/'v4_rev2/prebenchmark.py').is_file(), 'Invalid Rev2 implementation directory')


def file_hashes(results: Path) -> dict:
    return {name: sha256(results/name) for name in EXPERIMENT_FILES if (results/name).is_file()}


def q8_query_from_frozen(exp: Path, results: Path, addon: Path):
    """Require the exact frozen Q8 TS0 route already used in the confirmatory run."""
    from ts0_telemetry import guarded_inputs
    verified, measured, deployment, keyspace, queries, rows, predictions = guarded_inputs(exp, results, addon)
    q8_list = [q for q in queries if q.get('query_id') == 'Q8']
    ensure(len(q8_list) == 1, 'Expected exactly one frozen Q8 query')
    q8 = q8_list[0]
    ensure(deployment['query_routes']['Q8']['candidate_id'] == 'ts0_iot_raw',
           'Frozen Q8 route must use TS0')
    ensure(q8.get('value_lower') is None, 'Q8 source semantics unexpectedly changed')
    ensure(len(set(q8['devices'])) == len(q8['devices']), 'Duplicate Q8 devices')
    ensure(len(set(q8['sensors'])) == len(q8['sensors']), 'Duplicate Q8 sensors')
    ensure(q8['devices'] and q8['sensors'], 'Q8 requires device and sensor selection')
    return verified, measured, deployment, keyspace, q8, rows, predictions['Q8']


def evaluate_plan(plan, session, q8, route, expected, oracle_result, compare_result, execute_original):
    counted = CountingSession(session, {(d, s) for d in q8['devices'] for s in q8['sensors']})
    started = time.perf_counter_ns()
    if plan == 'A_FULL_SCAN':
        result = execute_original(counted, q8, route, list(q8['sensors']))
        fallbacks = 0
    elif plan == 'B_DESC_LIMIT1':
        result, fallbacks = optimized_q8(counted, q8, oracle_result)
    else:
        raise ValueError('Unknown plan: '+plan)
    elapsed = (time.perf_counter_ns() - started)/1e6
    ensure(compare_result(result, expected), 'Q8 oracle mismatch for ' + plan)
    return {'plan': plan, 'latency_ms': elapsed,
            'execute_calls': counted.execute_calls, 'iterated_rows': counted.iterated_rows,
            'fallback_count': fallbacks,
            'full_sql_calls': counted.sql_counts['FULL'],
            'limit1_sql_calls': counted.sql_counts['LIMIT1'],
            'oracle': 'PASS'}


def run(args):
    exp, results, addon, out = (p.resolve() for p in
                                (args.experiment, args.results, args.addon, args.out))
    check_paths(exp, results, addon, out)
    ensure(1 <= args.port <= 65535, 'Invalid Cassandra port')
    ensure(0 <= args.warmup <= 100, 'warmup must be between 0 and 100')
    ensure(2 <= args.pairs <= 2000 and args.pairs % 2 == 0,
           'pairs must be even and between 2 and 2000')
    ensure(1 <= args.fetch_size <= 5000, 'Invalid fetch_size')
    baseline_hashes = file_hashes(results)
    ensure(len(baseline_hashes) == len(EXPERIMENT_FILES), 'Original confirmatory outputs incomplete')
    verified, measured, deployment, ks, q8, source_rows, pred = q8_query_from_frozen(exp, results, addon)
    ensure(len(source_rows) <= args.max_cells, 'Safety limit exceeded')
    frozen_sha = measured['frozen_HBench_sha256']
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.stage9 import execute_query
    from v4_rev2 import prebenchmark as PB
    _result = source_oracle()  # Also configures the frozen A1 import path.
    expected = _result(source_rows, q8)
    from cassandra.cluster import Cluster
    cluster = Cluster([args.host], port=args.port)
    rows = []
    orders = balanced_orders(args.pairs, args.seed)
    try:
        session = cluster.connect(ks)
        session.default_fetch_size = args.fetch_size
        # Read-only metadata check; if table disappeared, do not recreate it.
        exists = session.execute(SELECT_TABLE, (ks, 'ts0_iot_raw')).one()
        ensure(exists is not None, 'Frozen TS0 table is missing; E5 does not materialize tables')
        route = deployment['query_routes']['Q8']
        plans = ('A_FULL_SCAN', 'B_DESC_LIMIT1')
        for plan in plans:
            observation = evaluate_plan(plan, session, q8, route, expected, _result, compare, execute_query)
            print('Preflight', plan, observation, flush=True)
            ensure(observation['fallback_count'] == 0,
                   'Unexpected Cassandra null rows in TS0; cannot compare frozen original correctly')
        for i in range(args.warmup):
            for plan in ('A_FULL_SCAN','B_DESC_LIMIT1') if i % 2 == 0 else ('B_DESC_LIMIT1','A_FULL_SCAN'):
                evaluate_plan(plan, session, q8, route, expected, _result, compare, execute_query)
        for pair_id, order in enumerate(orders, 1):
            for index, label in enumerate(order):
                plan = 'A_FULL_SCAN' if label == 'A' else 'B_DESC_LIMIT1'
                obs = evaluate_plan(plan, session, q8, route, expected, _result, compare, execute_query)
                obs.update({'pair_id': pair_id, 'order': order,
                            'position': index + 1})
                rows.append(obs)
                if obs['fallback_count']:
                    raise ValueError('Null fallback during timed run; abort rather than mixing strategies')
            if pair_id % 20 == 0:
                print(f'E5 completed {pair_id}/{args.pairs} balanced pairs', flush=True)
    finally:
        cluster.shutdown()
    # Verify all immutable inputs after database access; never mutate original data.
    PB.verify(exp)
    ensure(frozen_sha == sha256(exp/'outputs/v4_rev2_prebenchmark/stage8/28_hbench_v4_rev2_frozen.json'),
           'Frozen HBench changed after E5')
    ensure(file_hashes(results) == baseline_hashes,
           'Original confirmatory CSV or manifest changed after E5')
    ensure(len(rows) == args.pairs*2 and all(r['oracle'] == 'PASS' for r in rows),
           'Incomplete or incorrect E5 measurements')
    counts = Counter(r['plan'] for r in rows)
    ensure(counts['A_FULL_SCAN'] == args.pairs == counts['B_DESC_LIMIT1'], 'Unequal plan samples')
    stats = []
    for plan in ('A_FULL_SCAN','B_DESC_LIMIT1'):
        rr = [r for r in rows if r['plan'] == plan]
        ts = [r['latency_ms'] for r in rr]
        stats.append({'plan':plan,'n':len(ts),'median_ms':statistics.median(ts),
                      'mean_ms':statistics.mean(ts),
                      'p95_ms':percentile_nearest_rank(ts, 95),
                      'p99_ms':percentile_nearest_rank(ts, 99),
                      'median_execute_calls':statistics.median(r['execute_calls'] for r in rr),
                      'median_iterated_rows':statistics.median(r['iterated_rows'] for r in rr),
                      'max_fallbacks':max(r['fallback_count'] for r in rr)})
    by_pair = {}
    for row in rows:
        by_pair.setdefault(row['pair_id'], {})[row['plan']] = row
    diffs = []
    for pair_id in sorted(by_pair):
        pair = by_pair[pair_id]
        a = pair['A_FULL_SCAN']['latency_ms']; b = pair['B_DESC_LIMIT1']['latency_ms']
        diffs.append({'pair_id':pair_id,'order':pair['A_FULL_SCAN']['order'],
                      'full_ms':a,'limit1_ms':b,'limit1_minus_full_ms':b-a,
                      'full_over_limit1':a/max(b,1e-12)})
    diff_values = [r['limit1_minus_full_ms'] for r in diffs]
    med_diff = statistics.median(diff_values)
    # This is descriptive pairing only, not an independent-run confidence interval.
    assert not out.exists()
    out.mkdir(parents=True)
    write_csv(out/'e5_raw.csv', rows, ['pair_id','order','position','plan','latency_ms',
             'execute_calls','iterated_rows','fallback_count','full_sql_calls','limit1_sql_calls','oracle'])
    write_csv(out/'e5_paired.csv',diffs,['pair_id','order','full_ms','limit1_ms',
             'limit1_minus_full_ms','full_over_limit1'])
    write_csv(out/'e5_summary.csv',stats,list(stats[0]))
    manifest = {'status':'E5_READ_ONLY_Q8_PAIRED_COMPLETE',
        'exploratory_posthoc':True, 'not_retroactive_rev2_selection':True,
        'experiment':str(exp),'source_confirmatory_results':str(results),
        'frozen_hbench_sha256':frozen_sha,'confirmatory_results_hashes_before_after':baseline_hashes,
        'q8_input_sha256':hashlib.sha256(json.dumps(q8,sort_keys=True).encode()).hexdigest(),
        'selected_ts0_id':deployment['deployment_id'],'keyspace':ks,
        'pairs':args.pairs,'seed':args.seed,'warmup':args.warmup,'fetch_size':args.fetch_size,
        'balance':dict(Counter(orders)), 'client_latency_includes':'Python processing and Cassandra driver',
        'row_counter':'Python-iterated rows, not server-side reads',
        'call_counter':'Session.execute calls, not Cassandra paging/network roundtrips',
        'frozen_stage7_q8_prediction':{'request_count_proxy':pred['request_count_proxy'],
                                       'physical_rows_proxy':pred['physical_rows_proxy']},
        'median_paired_diff_limit1_minus_full_ms':med_diff,
        'source_code_sha256':sha256(Path(__file__)),
        'limitations':['Same TS0 data and physical table, no SQL engine official workload mix',
                       'One captured Java Q8 object, no Q8 parameter distribution',
                       'Repeated pairs in one run are not independent experimental campaigns',
                       'Prior Rev2 p95 cannot be replaced with new exploratory E5 p95',
                       'LIMIT 1 is valid for non-null TS0 sensor points; other storage semantics need tests',
                       'Paging and coordinator work are not captured by Session.execute counter']}
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    a,b = stats
    lines = ['# SchemaLens Rev2 — E5: Q8 TS0 FULL_SCAN versus DESC_LIMIT_1',
        '', '**Escopo:** controle pós-hoc, não altera HBench Rev2 e não usa nenhum resultado para nova seleção.',
        '', f'- Experimento confirmatório: `{exp}`',f'- HBench SHA-256: `{frozen_sha}`',
        f'- TS0: `{deployment["deployment_id"]}`; keyspace: `{ks}`',
        f'- Amostras por plano: **{args.pairs}**; AB/BA: **{len(orders)//2}/{len(orders)//2}**',
        '- **Todas as respostas foram validadas contra o oráculo lógico antes e depois das medições.**',
        '', '## Resultado', '',
        '| Plano | Média (ms) | Mediana (ms) | p95 (ms) | Chamadas execute | Linhas iteradas |',
        '|---|---:|---:|---:|---:|---:|',
        *[f'| {s["plan"]} | {s["mean_ms"]:.4f} | {s["median_ms"]:.4f} | '
          f'{s["p95_ms"]:.4f} | {s["median_execute_calls"]:.0f} | '
          f'{s["median_iterated_rows"]:.0f} |' for s in stats],
        '', f'- Diferença mediana pareada (LIMIT1 − FULL): **{med_diff:+.5f} ms**.',
        '- Interprete o p95 por plano como percentil de repetições neste controle, não p95 do workload.',
        '', '## Interpretação',
        '', '- `FULL_SCAN` é o Stage9 TS0 original congelado, executado sem alterações.',
        '- `DESC_LIMIT_1` usa exatamente a mesma tabela/partição, mas explora a ordem de clustering descendente.',
        '- A diferença isola principalmente **query plan/executor**, não uma nova família de esquema.',
        '- Se houver células Cassandra com `value_double=NULL`, a implementação otimizada tem fallback; não deve ser usada para alegar ganhos de LIMIT1 sem ensaio separado. A campanha atual bloqueia fallback.',
        '- Chamadas Python e linhas iteradas não representam trabalho interno do Cassandra.',
        '- Os dados e os coeficientes do Stage7/8 Rev2 continuam congelados; uma implementação definitiva pertence à futura Rev3.',
        '']
    (out/'RELATORIO_E5_PT.md').write_text('\n'.join(lines),encoding='utf-8')
    print('E5 COMPLETE:', {s['plan']:s['p95_ms'] for s in stats})
    print('Report:',out/'RELATORIO_E5_PT.md')
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment',required=True,type=Path)
    parser.add_argument('--results',required=True,type=Path)
    parser.add_argument('--addon',required=True,type=Path)
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=9043)
    parser.add_argument('--pairs',type=int,default=200,
                        help='Number of AB/BA balanced pairs (even). Each pair times both plans')
    parser.add_argument('--warmup',type=int,default=10)
    parser.add_argument('--seed',type=int,default=20261105)
    parser.add_argument('--fetch-size',type=int,default=128)
    parser.add_argument('--max-cells',type=int,default=100000)
    args = parser.parse_args()
    try:
        run(args)
    except Exception as exc:
        print('E5 FAILED:',repr(exc),file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
