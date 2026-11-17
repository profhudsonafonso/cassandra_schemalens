#!/usr/bin/env python3
"""SchemaLens Rev3 S2 read-only, pre-registered, auditable benchmark.

Commands (run from root of SchemaLens_IoT_Cassandra_V3_Automatico):
    python s2_benchmark.py preregister --project .
    python s2_benchmark.py run --project . --factor 1 --port 9043
    python s2_benchmark.py audit --project . --factor 1
    python s2_benchmark.py audit-all --project .

No DDL/DML, no flush or compaction, never overwrite benchmark evidence.
Timings are end-to-end Python client dispatch+result postprocessing, not server times.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

FACTORS = (1, 2, 4, 8)
BASE = 'BASELINE_Q8_OPTIMIZED'
U = 'R3H_38939db96444'
D = 'R3H_d2ea1974de53'
LABELS = (BASE, U, D)
B_SHA = '8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231'
SEED = 20261127  # Fixed prior to S2 timings (matches S1 seed default).
ROUNDS = 60
WARMUPS = 5
PAGE_SIZE = 16


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as file:
        for chunk in iter(lambda: file.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def write_new(path, obj):
    with Path(path).open('x', encoding='utf8') as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        f.write('\n')


def csv_new(path, data):
    if not data:
        raise ValueError('Empty CSV is not allowed')
    with Path(path).open('x', newline='', encoding='utf8') as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def csv_read(path):
    with Path(path).open(newline='', encoding='utf8') as f:
        return list(csv.DictReader(f))


def percentile(values, p):
    values = sorted(values)
    if not values:
        raise ValueError('Empty sample')
    return values[math.ceil(len(values) * p / 100) - 1]


def environment(project):
    project = project.resolve()
    s2 = project / 'experiments' / 'rev3_s2_partition_growth'
    p1 = load(s2 / 's1_preregister.json')
    draft_path = s2 / 's2_preregister_draft.json'
    draft = load(draft_path)
    if p1['frozen_rev3_B_sha256'] != B_SHA or draft['original_B_rev3_sha256'] != B_SHA:
        raise ValueError('B frozen-selection SHA mismatch')
    if draft['frozen_variants']['ts2_iot_wide_unbucketed'] != U or draft['frozen_variants']['ts2_iot_wide'] != D:
        raise ValueError('Frozen deployment IDs mismatch')
    if draft['density_factors'] != list(FACTORS) or draft['rounds_per_combo'] != ROUNDS or draft['warmups_per_combo'] != WARMUPS or draft['page_size'] != PAGE_SIZE:
        raise ValueError('S2 draft protocol differs from this benchmark implementation')
    cases_file = s2 / 's1_cases.json'
    if digest(cases_file) != p1['s1_cases_sha256']:
        raise ValueError('S1 case file mutated')
    cases = load(cases_file)
    if len(cases) != 18 or len({q['s1_case_id'] for q in cases}) != 18:
        raise ValueError('Expected exactly 18 distinct cases')
    if sum(q['query_id'] in ('Q2', 'Q7', 'Q9', 'Q12') for q in cases) != 16:
        raise ValueError('Missing temporal window cases')
    gen_path = s2 / 'generated' / 'generation_manifest.json'
    gen = load(gen_path)
    for factor in FACTORS:
        entry = gen['datasets'][str(factor)]
        path = s2 / 'generated' / entry['file']
        if digest(path) != entry['sha256'] or entry['cells'] != 54000 * factor:
            raise ValueError(f'Density {factor}x SHA/count mismatch')
        mat_file = s2 / 's2_materialized' / f'{factor}x' / 'materialization_manifest.json'
        semantic_file = s2 / 's2_materialized' / f'{factor}x' / 'semantic_validation.json'
        mat = load(mat_file)
        val = load(semantic_file)
        names = {BASE: f'sl_s2_d{factor}_ref', U: f'sl_s2_d{factor}_u', D: f'sl_s2_d{factor}_d'}
        if mat['status'] != 'S2_MATERIALIZATION_COMPLETE_NOT_BENCHMARKED' or mat['keyspaces'] != names or mat['dataset_sha256'] != entry['sha256']:
            raise ValueError(f'Density {factor}x materialization mismatch')
        if val['status'] != 'PASS_ALL' or val['validations'] != 54 or val['source_dataset_sha256'] != entry['sha256'] or val['case_sha256'] != digest(cases_file):
            raise ValueError(f'Density {factor}x semantic-validation mismatch')
        if len(val.get('results', [])) != 54 or any(r['status'] != 'PASS' for r in val['results']):
            raise ValueError('Incomplete semantic-validation records')
    experiment = Path(p1['original_B_experiment']).resolve()
    workspace = Path(p1['original_B_workspace']).resolve()
    if not experiment.is_dir() or not workspace.is_dir():
        raise FileNotFoundError('Frozen B experiment/workspace missing')
    return dict(project=project, s2=s2, p1=p1, draft_path=draft_path,
                cases_file=cases_file, cases=cases, gen_path=gen_path,
                gen=gen, experiment=experiment, workspace=workspace)


def prereg_file(e):
    return e['s2'] / 's2_benchmark_preregister.json'


def benchmark_dir(e, factor):
    return e['s2'] / 's2_benchmark' / f'{factor}x'


def preregister(e):
    dst = prereg_file(e)
    # All materializations and semantic audits must predate this preregistration.
    source_hashes = {}
    for factor in FACTORS:
        folder = e['s2'] / 's2_materialized' / f'{factor}x'
        source_hashes[str(factor)] = {
            'dataset_sha256': e['gen']['datasets'][str(factor)]['sha256'],
            'materialization_manifest_sha256': digest(folder / 'materialization_manifest.json'),
            'semantic_validation_sha256': digest(folder / 'semantic_validation.json'),
        }
        if benchmark_dir(e, factor).exists():
            raise FileExistsError(f'Benchmark evidence already exists for factor {factor}')
    if dst.exists():
        raise FileExistsError('S2 benchmark is already preregistered; do not overwrite it')
    record = {
        'status': 'S2_BENCHMARK_PREREGISTERED_BEFORE_TIMING',
        'method': 'POST_AB_EXPLORATORY_SYNTHETIC_FIXED_HISTORY_DENSITY_SWEEP',
        'frozen_B_sha256': B_SHA,
        'source_s1_cases_sha256': digest(e['cases_file']),
        's2_draft_sha256': digest(e['draft_path']),
        'generation_manifest_sha256': digest(e['gen_path']),
        'runner_sha256': digest(Path(__file__).resolve()),
        'original_B_workspace': str(e['workspace']),
        'original_B_experiment': str(e['experiment']),
        'inputs': source_hashes,
        'factors': list(FACTORS),
        'labels': list(LABELS),
        'rounds': ROUNDS, 'warmups': WARMUPS,
        'page_size': PAGE_SIZE, 'measurement_seed': SEED,
        'cases_per_factor': 18,
        'expected_total_observations': 4 * 18 * 3 * ROUNDS,
        'randomization': 'shuffle cases and variant order on every round; alternate variant reversal',
        'timing': 'perf_counter_ns of client dispatch, full fetch and postprocessing; oracle comparison excluded',
        'no_ddl_dml_flush_or_compaction': True,
        'percentile': 'nearest_rank',
        'interpretation': 'synthetic densification of Java-derived source; not independent official IoT Benchmark replay',
    }
    write_new(dst, record)
    print('PASS preregistration BEFORE S2 timing:', dst)
    print('Source variants:', ', '.join(LABELS))
    print('Expected measured rows:', record['expected_total_observations'])


def verified_prereg(e):
    p = load(prereg_file(e))
    if p['status'] != 'S2_BENCHMARK_PREREGISTERED_BEFORE_TIMING' or p['frozen_B_sha256'] != B_SHA:
        raise ValueError('Missing/mismatched S2 benchmark preregistration')
    if p['runner_sha256'] != digest(Path(__file__).resolve()):
        raise ValueError('Runner changed after S2 preregistration')
    if p['source_s1_cases_sha256'] != digest(e['cases_file']) or p['s2_draft_sha256'] != digest(e['draft_path']) or p['generation_manifest_sha256'] != digest(e['gen_path']):
        raise ValueError('Source manifests or workload mutated after preregistration')
    for factor in FACTORS:
        folder = e['s2'] / 's2_materialized' / f'{factor}x'
        hashes = p['inputs'][str(factor)]
        if hashes['dataset_sha256'] != e['gen']['datasets'][str(factor)]['sha256'] or hashes['materialization_manifest_sha256'] != digest(folder/'materialization_manifest.json') or hashes['semantic_validation_sha256'] != digest(folder/'semantic_validation.json'):
            raise ValueError(f'Factor {factor}x input changed after preregistration')
    return p


def prepare_runtime(e):
    sys.path[:0] = [str(e['experiment']),
        str(e['project'] / 'SchemaLens_Rev3_Independent_Confirmatory'),
        str(e['project'] / 'SchemaLens_V4_Rev3_PlanAware'),
        str(e['project'] / 'SchemaLens_V4_Rev2_Confirmatory')]
    from rev3_confirmatory import gate
    from rev3_confirmatory.benchmark import baseline_dispatch
    from rev3.runtime import dispatch
    from iot_pipeline.shared import read_rows
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from v4_rev2.runtime_helpers import DAY
    exp, protocol, selected = gate.verify(e['workspace'])
    if Path(exp).resolve() != e['experiment'] or protocol['rev3_selection_sha256'] != B_SHA:
        raise ValueError('Frozen B gate mismatch')
    designs = {h['deployment_id']: h for h in selected}
    if not set((U,D)).issubset(designs):
        raise ValueError('TS2-U / TS2-D missing in frozen selection')
    catalog_file = e['experiment']/gate.REV3/'stage5'/'15_executable_plan_catalog.json'
    catalog = {p['plan_id']:p for p in load(catalog_file)}
    return baseline_dispatch,dispatch,read_rows,source_oracle,compare,DAY,designs,catalog


def run_factor(e, p, factor, host, port):
    if host not in ('127.0.0.1', 'localhost', '::1') or not (1 <= port <= 65535):
        raise ValueError('S2 runner restricted to local Cassandra port')
    dest = benchmark_dir(e, factor)
    if dest.exists():
        raise FileExistsError(f'Refusing to overwrite existing S2 benchmark directory {dest}')
    baseline_dispatch, dispatch, read_rows, source_oracle, compare, DAY, designs, catalog = prepare_runtime(e)
    entry = e['gen']['datasets'][str(factor)]
    rows = read_rows(e['s2'] / 'generated' / entry['file'])
    if len(rows) != factor * 54000:
        raise ValueError('Unexpected density row count')
    sensors = sorted({r['sensor_name'] for r in rows})
    days = {r['timestamp'] // DAY for r in rows}
    oracle = source_oracle()
    expected = {q['s1_case_id']:oracle(rows,q) for q in e['cases']}
    counts = {case: len(results) for case, results in expected.items()}
    names = {BASE: f'sl_s2_d{factor}_ref', U: f'sl_s2_d{factor}_u', D: f'sl_s2_d{factor}_d'}
    original_audit = load(e['s2'] / 's2_materialized' / f'{factor}x' / 'semantic_validation.json')
    recorded_access = {(r['case_id'], r['variant']):
                       (int(r['execute_calls']), int(r['iterated_rows']),r['plan'])
                       for r in original_audit['results']}
    if len(recorded_access) != 54:
        raise ValueError('Inconsistent prebenchmark telemetry entries')
    from cassandra.cluster import Cluster
    cluster = Cluster([host], port=port)
    raw, orders = [], []
    failure = None
    started = time.time()
    dest.mkdir(parents=True, exist_ok=False)
    try:
        session = cluster.connect()
        present = {r.keyspace_name for r in session.execute('SELECT keyspace_name FROM system_schema.keyspaces')}
        if not set(names.values()).issubset(present):
            raise ValueError('One or more new S2 keyspaces missing')
        def execute(q,label):
            if label == BASE:
                return baseline_dispatch(session,q,True,sensors,days,PAGE_SIZE,oracle)
            return dispatch(session,q,designs[label],catalog,sensors,days,PAGE_SIZE,oracle)
        # Verify against current database BEFORE any latency measurement.
        for q in e['cases']:
            for label in LABELS:
                session.set_keyspace(names[label])
                got, trace = execute(q,label)
                case = q['s1_case_id']
                if not compare(got,expected[case]):
                    raise AssertionError(f'Pretiming semantic mismatch: {case} {label}')
                access = (trace['execute_calls'],trace['iterated_rows'],trace['physical_plan'])
                if access != recorded_access[case,label]:
                    raise AssertionError(f'Pretiming access trace changed: {case} {label}')
        print(f'PASS: factor {factor}x pretiming oracles 54/54',flush=True)
        # Warm-up before first timed round. No baseline oracle calls inside timed region.
        for q in e['cases']:
            for label in LABELS:
                session.set_keyspace(names[label])
                for _ in range(WARMUPS):
                    got,trace=execute(q,label)
                    if not compare(got,expected[q['s1_case_id']]):
                        raise AssertionError('Warmup semantic mismatch')
        rng = random.Random(SEED + factor)
        for rep in range(1,ROUNDS+1):
            queue = list(e['cases']); rng.shuffle(queue)
            for q in queue:
                case = q['s1_case_id']
                variants = list(LABELS); rng.shuffle(variants)
                if rep % 2 == 0:
                    variants.reverse()
                orders.append(dict(factor=factor,case_id=case,rep=rep,variants_order='|'.join(variants)))
                for pos,label in enumerate(variants,1):
                    session.set_keyspace(names[label])
                    t0 = time.perf_counter_ns()
                    got, trace = execute(q,label)
                    elapsed = (time.perf_counter_ns()-t0) / 1e6
                    if not compare(got,expected[case]):
                        raise AssertionError(f'Timed semantic mismatch: {case}/{label}/{rep}')
                    if (trace['execute_calls'],trace['iterated_rows'],trace['physical_plan']) != recorded_access[case,label]:
                        raise AssertionError(f'Timed access trace changed: {case}/{label}/{rep}')
                    raw.append(dict(factor=factor,case_id=case,query_id=q['query_id'],
                        window_days=q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                        variant=label,keyspace=names[label],rep=rep,position=pos,
                        latency_ms=round(elapsed,9),execute_calls=trace['execute_calls'],
                        iterated_rows=trace['iterated_rows'],oracle_cardinality=counts[case],
                        physical_plan=trace['physical_plan'],correctness='PASS'))
            if rep%10==0:
                print(f'S2 factor={factor}x rounds={rep}/{ROUNDS} observations={len(raw)}',flush=True)
        if len(raw) != 18*3*ROUNDS or len(orders) != 18*ROUNDS:
            raise AssertionError('Measured sample/order count mismatch')
    except BaseException as exc:
        failure = exc
        write_new(dest/'failure_manifest.json',dict(status='FAILED_PRESERVE_EVIDENCE',
            error_type=type(exc).__name__,error=str(exc),partial_samples=len(raw),
            timestamp_epoch=time.time(),no_overwrite=True))
        if raw:
            csv_new(dest/'partial_latencies.csv',raw)
        if orders:
            csv_new(dest/'partial_orders.csv',orders)
        raise
    finally:
        cluster.shutdown()
    summary = summarize(raw, factor)
    csv_new(dest/'raw_latencies.csv',raw)
    csv_new(dest/'orders.csv',orders)
    csv_new(dest/'query_p95.csv',summary)
    manifest = dict(status='S2_BENCHMARK_COMPLETE', factor=factor,
        raw_measurements=len(raw),case_variant_groups=len(summary),
        expected_measurements=18*3*ROUNDS,warmups=18*3*WARMUPS,
        benchmark_preregister_sha256=digest(prereg_file(e)),
        dataset_sha256=entry['sha256'],s1_cases_sha256=digest(e['cases_file']),
        frozen_selection_sha256=B_SHA,host=host,port=port,
        started_epoch_seconds=started,ended_epoch_seconds=time.time(),
        comparison_scope='S2 synthetic densification, post-A/B exploratory, client end-to-end latency',
        performance_not_equivalent_to_official_java_benchmark=True,
        files_sha256={name:digest(dest/name) for name in ('raw_latencies.csv','orders.csv','query_p95.csv')})
    write_new(dest/'manifest.json',manifest)
    print(f'COMPLETE factor={factor}x 3240 observations. Run audit.',flush=True)


def summarize(raw,factor):
    groups=defaultdict(list)
    for r in raw:
        groups[r['case_id'],r['variant']].append(r)
    if len(groups)!=54:
        raise ValueError(f'Expected 54 case/variant groups; found {len(groups)}')
    summary=[]
    for (case,label),rr in sorted(groups.items()):
        if len(rr)!=ROUNDS:
            raise ValueError('Incomplete measurement group')
        a=[float(x['latency_ms']) for x in rr]
        example=rr[0]
        summary.append(dict(factor=factor,case_id=case,query_id=example['query_id'],
            window_days=example['window_days'],variant=label,runs=ROUNDS,
            mean_ms=round(statistics.mean(a),9),median_ms=round(statistics.median(a),9),
            p95_ms=round(percentile(a,95),9),p99_ms=round(percentile(a,99),9),
            execute_calls=example['execute_calls'],iterated_rows=example['iterated_rows'],
            oracle_cardinality=example['oracle_cardinality'],correctness='PASS'))
    return summary


def audit_factor(e,p,factor):
    dest=benchmark_dir(e,factor)
    manifest=load(dest/'manifest.json')
    if manifest['status']!='S2_BENCHMARK_COMPLETE' or manifest['factor']!=factor or manifest['benchmark_preregister_sha256'] != digest(prereg_file(e)):
        raise ValueError(f'Missing/invalid complete benchmark manifest for {factor}x')
    for name, expected in manifest['files_sha256'].items():
        if digest(dest/name)!=expected:
            raise ValueError('Result file SHA changed: '+name)
    raw,orders,summary=csv_read(dest/'raw_latencies.csv'),csv_read(dest/'orders.csv'),csv_read(dest/'query_p95.csv')
    if len(raw)!=3240 or len(orders)!=1080 or len(summary)!=54:
        raise ValueError('Wrong S2 sample/order/summary counts')
    case_ids={q['s1_case_id'] for q in e['cases']}
    positions={}
    for o in orders:
        key=(o['case_id'],int(o['rep']))
        perm=o['variants_order'].split('|')
        if key in positions or key[0] not in case_ids or sorted(perm)!=sorted(LABELS):
            raise ValueError('Wrong or duplicate randomized permutation')
        positions[key]=perm
    if len(positions)!=1080:
        raise ValueError('Missing randomized orders')
    unique=set()
    for r in raw:
        key=(r['case_id'],r['variant'],int(r['rep']))
        if key in unique or key[0] not in case_ids or r['variant'] not in LABELS or r['correctness']!='PASS':
            raise ValueError('Invalid or duplicate measured sample')
        unique.add(key)
        if r['keyspace'] != {BASE:f'sl_s2_d{factor}_ref',U:f'sl_s2_d{factor}_u',D:f'sl_s2_d{factor}_d'}[r['variant']]:
            raise ValueError('Wrong S2 keyspace in measurement')
        if int(r['factor'])!=factor or int(r['position']) not in (1,2,3) or positions[r['case_id'],int(r['rep'])][int(r['position'])-1] != r['variant']:
            raise ValueError('Measurement / randomization mismatch')
        if float(r['latency_ms'])<=0 or not math.isfinite(float(r['latency_ms'])):
            raise ValueError('Non-positive or nonfinite measured latency')
    actual=summarize(raw,factor)
    originals={(r['case_id'],r['variant']):r for r in summary}
    for r in actual:
        old=originals.get((r['case_id'],r['variant']))
        if old is None:
            raise ValueError('Missing p95 summary row')
        for k in ('p95_ms','mean_ms','median_ms','p99_ms'):
            if not math.isclose(float(r[k]),float(old[k]),rel_tol=1e-9,abs_tol=1e-7):
                raise ValueError('Summary does not match raw records: '+k)
    report = dict(status='PASS',factor=factor,samples=len(raw),orders=len(orders),
                  groups=len(actual),recomputed_metrics='PASS',data_sha256=manifest['dataset_sha256'])
    report_file=dest/'audit.json'
    if report_file.exists():
        if load(report_file)!=report:
            raise ValueError('Existing audit differs')
    else:
        write_new(report_file,report)
    print(f'PASS audit {factor}x: {len(raw)} latencies, {len(actual)} groups, {len(orders)} orders')
    return actual


def audit_all(e,p):
    byfactor={factor:audit_factor(e,p,factor) for factor in FACTORS}
    records=[]
    for factor,rows in byfactor.items():
        index={(r['case_id'],r['variant']):r for r in rows}
        for q in e['cases']:
            case=q['s1_case_id']
            b,u,d=(index[case,label] for label in LABELS)
            pu,pd,pb=(float(x['p95_ms']) for x in (u,d,b))
            records.append(dict(factor=factor,case_id=case,query_id=q['query_id'],
                window_days=q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                baseline_p95_ms=pb,ts2u_p95_ms=pu,ts2d_p95_ms=pd,
                d_over_u_ratio=round(pd/pu,9),d_over_u_pct=round((pd/pu-1)*100,5),
                winner=('TS2-U' if pu<pd else 'TS2-D' if pd<pu else 'TIE'),
                ts2u_execute_calls=u['execute_calls'],ts2d_execute_calls=d['execute_calls'],
                ts2u_iterated_rows=u['iterated_rows'],ts2d_iterated_rows=d['iterated_rows']))
    target=e['s2']/'s2_benchmark'/'all_density_comparison.csv'
    if target.exists():
        raise FileExistsError('Full S2 comparison already exists; do not overwrite')
    csv_new(target,records)
    print('PASS ALL: 12,960 measured observations; comparison:',target)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('preregister','run','audit','audit-all'))
    parser.add_argument('--project',type=Path,default=Path.cwd())
    parser.add_argument('--factor',type=int,choices=FACTORS)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=9043)
    args=parser.parse_args()
    e=environment(args.project)
    if args.action=='preregister':
        preregister(e)
    else:
        p=verified_prereg(e)
        if args.action in ('run','audit') and args.factor is None:
            parser.error('--factor required for run/audit')
        if args.action=='run':
            run_factor(e,p,args.factor,args.host,args.port)
        elif args.action=='audit':
            audit_factor(e,p,args.factor)
        else:
            audit_all(e,p)


if __name__=='__main__':
    main()
