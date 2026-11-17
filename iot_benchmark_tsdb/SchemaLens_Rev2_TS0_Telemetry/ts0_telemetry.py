#!/usr/bin/env python3
"""Read-only post-hoc TS0 instrumentation for SchemaLens V4 Rev2.

This diagnostic replays only the already-frozen TS0 routes. It measures how many
calls to Python Cassandra Session.execute are issued and how many row objects
are consumed by the unchanged V3 Stage9 TS0 executor. No timed benchmark,
DDL, INSERT, UPDATE, DELETE, FLUSH, or mutation of original artifacts.

All code/comments in English; Portuguese guidance is in README_PT.md.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
import csv
import hashlib
import json
from pathlib import Path
import re
import statistics
import sys

REV2 = Path('outputs/v4_rev2_prebenchmark')
TS0 = 'ts0_iot_raw'
KS_RE = re.compile(r'sl_conf_r2_[0-9a-f]{8}_r2h_[0-9a-f]{12}\Z')
SQL_RE = re.compile(
    r'SELECT timestamp_ms,value_double FROM ts0_iot_raw '
    r'WHERE device_id=%s AND sensor_name=%s'
    r'(?: AND timestamp_ms >= %s AND timestamp_ms < %s)?\Z',
    re.ASCII,
)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def jread(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def csv_write(path, records, fields):
    with Path(path).open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


def require(cond, msg):
    if not cond:
        raise ValueError(msg)


class CountedRows:
    """Count rows actually iterated, not Cassandra rows fetched into pages."""
    def __init__(self, underlying, counter):
        self.underlying = underlying
        self.counter = counter

    def __iter__(self):
        for row in self.underlying:
            self.counter['iterated_rows'] += 1
            yield row


class ReadOnlyCountingSession:
    """Transparent subset of Session for the immutable Stage9 TS0 SELECT path."""
    def __init__(self, session):
        self.session = session
        self.calls = 0
        self.iterated_rows = 0
        self.sql_fingerprints = set()

    def execute(self, statement, parameters=None, *args, **kwargs):
        # Fail closed: Stage9 TS0 must issue exactly one of its two known SELECTs.
        require(isinstance(statement, str) and SQL_RE.fullmatch(statement) is not None,
                'Unsafe or changed TS0 query shape; instrumenter accepts ONLY V3 Stage9 TS0 SELECT')
        required_params = 4 if 'timestamp_ms >=' in statement else 2
        require(isinstance(parameters, (tuple, list)) and len(parameters) == required_params,
                'TS0 SELECT bind parameters no longer match frozen query shape')
        require(not args and not kwargs, 'Unexpected Session.execute options')
        self.calls += 1
        self.sql_fingerprints.add(hashlib.sha256(statement.encode()).hexdigest()[:16])
        counter = {'iterated_rows': 0}
        result = self.session.execute(statement, parameters)
        return _AccumulateRows(result, counter, self)


class _AccumulateRows(CountedRows):
    def __init__(self, underlying, counter, owner):
        super().__init__(underlying, counter)
        self.owner = owner

    def __iter__(self):
        for row in super().__iter__():
            self.owner.iterated_rows += 1
            yield row


def safe_output(out, exp, results, addon):
    out = Path(out).resolve()
    for protected in (Path(exp).resolve(), Path(results).resolve(), Path(addon).resolve()):
        require(not out.is_relative_to(protected), 'Output cannot be inside immutable inputs: ' + str(protected))
    require(not out.exists(), 'Refusing to overwrite telemetry output: ' + str(out))
    return out


def choose_ts0(frozen, measured):
    chosen = [h for h in frozen if h.get('canonical_source') == TS0]
    require(len(chosen) == 1, 'Exactly one frozen TS0 canonical deployment expected')
    h = chosen[0]
    did = h['deployment_id']
    require(did in measured['selected_ids'], 'Measured manifest did not contain frozen TS0 deployment')
    require(len(h['candidates']) == 1 and h['candidates'][0] == TS0,
            'Frozen TS0 deployment has other materializations; this diagnostic is TS0-only')
    require(len(h['query_routes']) == 12 and set(h['query_routes']) == {f'Q{i}' for i in range(1,13)},
            'Frozen TS0 deployment must contain Q1-Q12')
    for qid, route in h['query_routes'].items():
        require(route['family'] == 'TS0' and route['candidate_id'] == TS0
                and route['route_status'] == 'DIRECT',
                'Frozen TS0 route changed: ' + qid)
    ks = measured['keyspaces'].get(did)
    require(isinstance(ks, str) and KS_RE.fullmatch(ks), 'Keyspace is not an allowed confirmatory TS0 keyspace')
    require(ks.endswith(did.lower()), 'Frozen TS0 ID and Cassandra keyspace disagree')
    return h, ks


def summarize(pred, counts):
    """Predicted row/execute count and observed repeated counts per query."""
    require(counts, 'Missing instrumented repetitions')
    calls = [int(x['execute_calls']) for x in counts]
    visited = [int(x['iterated_rows']) for x in counts]
    pred_calls = int(pred['request_count_proxy'])
    pred_rows = int(pred['physical_rows_proxy'])
    if len(set(calls)) > 1 or len(set(visited)) > 1:
        status = 'VARIABLE_OBSERVED_COUNTS'
    elif calls[0] == pred_calls and visited[0] == pred_rows:
        status = 'MATCH'
    else:
        status = 'MISMATCH'
    return {
        'query_id': counts[0]['query_id'],
        'deployment_id': counts[0]['deployment_id'],
        'pred_execute_calls': pred_calls,
        'observed_execute_calls': statistics.median(calls),
        'delta_calls': statistics.median(calls) - pred_calls,
        'pred_physical_rows': pred_rows,
        'observed_iterated_rows': statistics.median(visited),
        'delta_rows': statistics.median(visited) - pred_rows,
        'min_calls': min(calls), 'max_calls': max(calls),
        'min_iterated_rows': min(visited), 'max_iterated_rows': max(visited),
        'repetitions': len(counts), 'status': status,
        'note': 'Session.execute calls are NOT actual Cassandra coordinator/network round trips',
    }


def guarded_inputs(exp, results, addon):
    """Validate freeze and previously completed benchmark before connecting."""
    from v4_rev2 import prebenchmark as P
    from v4_auto import selector as A
    original_meta = P.verify(exp)
    require(original_meta.get('independent_from_original_v3') is True,
            'TS0 telemetry requires the independently frozen confirmatory Rev2 experiment')
    measured = jread(results/'manifest.json')
    require(measured['status'] == 'CONFIRMATORY_CASSANDRA_COMPLETE',
            'Original measured run not complete')
    expected = digest(exp/REV2/'stage8/28_hbench_v4_rev2_frozen.json')
    require(expected == measured['frozen_HBench_sha256'], 'HBench SHA differs from original measurements')
    require(digest(exp/REV2/'stage8/manifest.json') == measured['frozen_manifest_sha256'],
            'Selection manifest hash differs from original measurement')
    require(original_meta['dataset_query_hashes']['data'] == measured['dataset_sha256'],
            'Dataset hash differs from original measurement')
    require(original_meta['dataset_query_hashes']['queries'] == measured['queries_sha256'],
            'Queries hash differs from original measurement')
    chosen = A.jread(exp/REV2/'stage8/28_hbench_v4_rev2_frozen.json')
    h, ks = choose_ts0(chosen, measured)
    from iot_pipeline.shared import read, read_rows, ROOT
    require(Path(ROOT).resolve() == exp.resolve(),
            'Wrong iot_pipeline imported; run from isolated experiment root')
    q = read(exp/'phase_a1/outputs/query_instances_pilot.json')
    rows = read_rows(exp/'phase_a1/outputs/normalized_double_readings.csv')
    require(len(q) == 12 and set(x['query_id'] for x in q) == set(h['query_routes']),
            'Changed or incomplete Q1-Q12 workload')
    plans = A.jread(exp/REV2/'stage7/23_read_proxies_rev2.json')
    pred = {x['query_id']: x for x in plans if x['candidate_id'] == TS0}
    require(set(pred) == set(h['query_routes']), 'Frozen Stage7 TS0 predictions missing')
    for qid, route in h['query_routes'].items():
        require(pred[qid]['plan_id'] == route['plan_id'], 'TS0 plan mismatch: '+qid)
    return original_meta, measured, h, ks, q, rows, pred


def run(args):
    exp = args.experiment.resolve()
    results = args.results.resolve()
    addon = args.addon.resolve()
    out = safe_output(args.out, exp, results, addon)
    require(1 <= args.rounds <= 10, 'Use 1..10 repetitions; this is instrumentation, not timing')
    require(1 <= args.port <= 65535, 'Invalid Cassandra port')
    require(args.max_cells > 0, 'Invalid cell safety limit')
    original_meta, measured, h, keyspace, queries, rows, pred = guarded_inputs(exp, results, addon)
    require(len(rows) <= args.max_cells, 'Too many cells for conservative TS0-only diagnostic')
    measured_hashes_before = {name:digest(results/name) for name in ('manifest.json','raw_latencies.csv','query_p95.csv')}
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.stage9 import execute_query
    from cassandra.cluster import Cluster
    oracle = source_oracle()
    sensors = sorted({r['sensor_name'] for r in rows})
    samples = []
    cluster = Cluster([args.host], port=args.port)
    try:
        session = cluster.connect(keyspace)
        # Additional gate: do not generate a new TS0 table, never write.
        table = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
                                (keyspace, TS0)).one()
        require(table is not None, 'Frozen TS0 table no longer present; do not recreate it')
        for q in queries:
            qid = q['query_id']
            expected = oracle(rows, q)
            for rep in range(1, args.rounds+1):
                counting = ReadOnlyCountingSession(session)
                got = execute_query(counting, q, h['query_routes'][qid], sensors)
                require(compare(got, expected), 'TS0 result changed; abort: ' + qid)
                samples.append({'query_id':qid,'deployment_id':h['deployment_id'],'rep':rep,
                                'execute_calls':counting.calls,'iterated_rows':counting.iterated_rows,
                                'oracle_correct':'PASS',
                                'statement_fingerprints':'|'.join(sorted(counting.sql_fingerprints))})
            print('Read-only TS0 telemetry:', qid, 'calls=',counting.calls,
                  'iterated rows=',counting.iterated_rows, flush=True)
    finally:
        cluster.shutdown()
    require(original_meta == __import__('v4_rev2.prebenchmark',fromlist=['verify']).verify(exp),
            'Frozen inputs changed during TS0 telemetry')
    require(all(digest(results/name)==hashval for name,hashval in measured_hashes_before.items()),
            'Original confirmatory result files changed during TS0 telemetry')
    byq = defaultdict(list)
    for x in samples:
        byq[x['query_id']].append(x)
    comparisons = [summarize(pred[qid], byq[qid]) for qid in sorted(byq,key=lambda k:int(k[1:]))]
    statuses = {s:sum(x['status']==s for x in comparisons) for s in ('MATCH','MISMATCH','VARIABLE_OBSERVED_COUNTS')}
    record_hashes = measured_hashes_before
    frozen_hash = digest(exp/REV2/'stage8/28_hbench_v4_rev2_frozen.json')
    out.mkdir(parents=True,exist_ok=False)
    csv_write(out/'ts0_counts_raw.csv',samples,
              ['query_id','deployment_id','rep','execute_calls','iterated_rows','oracle_correct','statement_fingerprints'])
    csv_write(out/'ts0_stage7_vs_observed.csv',comparisons,list(comparisons[0]))
    manifest = {'status':'READ_ONLY_TS0_TELEMETRY_COMPLETE','version':'TS0_TELEMETRY_V1',
       'experiment':str(exp),'source_results':str(results),'repetitions':args.rounds,
       'original_confirmatory_results_sha256':record_hashes,
       'frozen_rev2_hbench_sha256':frozen_hash,
       'frozen_ts0_deployment_id':h['deployment_id'],'keyspace':keyspace,
       'stage7_status_counts':statuses,'executed_read_only':True,
       'read_only_scope':'Only SELECT on system_schema.tables and frozen ts0_iot_raw; no DDL, writes or flush',
       'oracle_correct_for_all_runs':True,
       'source_code_sha256':digest(Path(__file__)),
       'limitations':['No latency measurement and no model refitting',
          'Session.execute count is NOT network/Cassandra paging round trips',
          'iterated_rows is consumed Python row objects, not server-side tombstones/bytes',
          'Uses existing post-flush physical database and may affect cache, not frozen CSV measurements']}
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    lines = ['# SchemaLens Rev2 — Telemetria TS0 (pós-avaliação confirmatória)',
      '', f'- **Estado:** {manifest["status"]}',
      f'- HBench: `{frozen_hash}`',
      f'- Deployment: `{h["deployment_id"]}`',
      f'- Quantidade de consultas: {len(comparisons)}; repetições por consulta: {args.rounds}',
      f'- Comparações: **{statuses}**',
      '- Consultas executadas: somente SELECT, sobre a TS0 já materializada.',
      '', '## Stage 7: previsão versus linhas realmente iteradas', '',
      '| Consulta | Chamadas previstas | Chamadas observadas | Linhas previstas | Linhas observadas | Situação |',
      '|---|---:|---:|---:|---:|---|']
    for x in comparisons:
        lines.append(f'| {x["query_id"]} | {x["pred_execute_calls"]} | {int(x["observed_execute_calls"])} | '
                     f'{x["pred_physical_rows"]} | {int(x["observed_iterated_rows"])} | {x["status"]} |')
    lines += ['', '## Interpretação', '',
      '- **MATCH** significa igualdade das contagens de `Session.execute` e `iterated_rows` com os proxies congelados.',
      '- **MISMATCH** é uma divergência registrada, não uma falha a ser corrigida dentro do HBench Rev2.',
      '- **VARIABLE_OBSERVED_COUNTS** indica que as repetições não foram estáveis.',
      '- Os contadores não medem bytes de rede, SSTable bytes ou solicitações internas de paginação.',
      '- Latências já registradas e os coeficientes Rev2 permanecem inalterados.',
      '', '## Próximo experimento', '',
      'Com todas as 36 rotas agora instrumentadas (24 existentes + 12 TS0), planejar uma nova geração Java de crescimento ',
      'com mais sensores, maior número de dispositivos, diferentes larguras de consulta e sementes independentes.',
      'Congelar uma nova revisão antes de quaisquer novas medições. Não reutilizar esta observação para reavaliar a seleção Rev2.',
      '']
    (out/'RELATORIO_TS0_PT.md').write_text('\n'.join(lines),encoding='utf-8')
    print('TS0 AUDIT COMPLETE:',statuses)
    print('Report:',out/'RELATORIO_TS0_PT.md')
    return comparisons


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--experiment',required=True,type=Path)
    ap.add_argument('--results',required=True,type=Path)
    ap.add_argument('--addon',required=True,type=Path)
    ap.add_argument('--out',required=True,type=Path)
    ap.add_argument('--host',default='127.0.0.1')
    ap.add_argument('--port',type=int,default=9043)
    ap.add_argument('--rounds',type=int,default=3)
    ap.add_argument('--max-cells',type=int,default=100000)
    args = ap.parse_args()
    try:run(args)
    except Exception as e:
        print('TS0 TELEMETRY FAILED:',e,file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
