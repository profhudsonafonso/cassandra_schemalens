#!/usr/bin/env python3
"""Fase 3, E10 (EMENDA_04): server-side decomposition of query time with Cassandra request tracing.

    python fase3_trace.py run        # writes experiments/rev3_fase2/e10/ (refuses to overwrite)

Tracing probability is set to 1 only for the sample and always reset to 0 afterwards. Executions are sequential from one
client, so every traced session that starts inside an execution's time interval belongs to that execution.
"""
from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

import fase3_core  # noqa: F401  (registers Fase 3 campaigns and the P1 case subset)
import fase2_core as C

OUT = C.F2 / 'e10'
SAMPLE = [('e3', 'h3', ('Q2', 'Q12'), (1, 7, 21, 80)), ('e9', 'p1', ('Q2', 'Q12'), (1, 7, 21))]
VARIANTS = (C.U, C.D)
REPS = 5


def trace_probability(p):
    if p not in (0, 1):
        raise ValueError('only 0 or 1')
    C._docker(['exec', C.S3.CONTAINER, 'nodetool', 'settraceprobability', str(p)])
    got = C._docker(['exec', C.S3.CONTAINER, 'nodetool', 'gettraceprobability'])
    C.log('trace probability ->', got.strip())


def main():
    if sys.argv[1:] != ['run']:
        sys.exit(__doc__)
    OUT.mkdir(parents=True, exist_ok=False)
    raw = []
    t_begin = time.time()
    try:
        trace_probability(1)
        for camp, ds, queries, windows in SAMPLE:
            info, sensors, days, oracle, compare, cases, expected = C.oracle_bundle(ds)
            chosen = [q for q in cases if q['query_id'] in queries and q['s1_window_days'] in windows]
            cluster, session = C.connect()
            try:
                execute = C.make_executor(session, sensors, days, oracle)
                for q in chosen:
                    for label in VARIANTS:
                        session.set_keyspace(C.ks_name(camp, ds, 'sst', label))
                        for rep in range(1, REPS + 1):
                            t0 = time.time()
                            p0 = time.perf_counter_ns()
                            got, tr = execute(q, label)
                            ms = (time.perf_counter_ns() - p0) / 1e6
                            t1 = time.time()
                            if not compare(got, expected[q['s1_case_id']]):
                                raise AssertionError('semantic mismatch under tracing')
                            raw.append(dict(dataset=ds, case_id=q['s1_case_id'], variant=C.SHORT[label], rep=rep,
                                            start=t0, end=t1, client_ms=ms, execute_calls=tr['execute_calls'],
                                            iterated_rows=tr['iterated_rows']))
                        C.log(f'{ds} {q["s1_case_id"]} {C.SHORT[label]}: {REPS} traced executions')
            finally:
                cluster.shutdown()
    finally:
        trace_probability(0)
    time.sleep(15)  # traces are written asynchronously
    cluster, session = C.connect()
    try:
        sess = [(r.started_at.timestamp(), r.duration) for r in
                session.execute('SELECT started_at, duration FROM system_traces.sessions')
                if r.started_at and r.started_at.timestamp() >= t_begin - 5 and r.duration is not None]
    finally:
        cluster.shutdown()
    sess.sort()
    import bisect
    starts = [s for s, _ in sess]
    for r in raw:
        i, j = bisect.bisect_left(starts, r['start'] - 0.002), bisect.bisect_right(starts, r['end'] + 0.002)
        r['server_requests'] = j - i
        r['server_us_sum'] = sum(d for _, d in sess[i:j])
        r['server_fraction'] = round(r['server_us_sum'] / 1000 / r['client_ms'], 4) if r['client_ms'] else None
    C.csv_new(OUT / 'trace_raw.csv', raw)
    groups = {}
    for r in raw:
        groups.setdefault((r['dataset'], r['case_id'], r['variant']), []).append(r)
    summ = []
    for (ds, case, v), rr in sorted(groups.items()):
        med = lambda k: statistics.median(x[k] for x in rr)
        req = med('server_requests')
        summ.append(dict(dataset=ds, case_id=case, variant=v, calls=rr[0]['execute_calls'], rows=rr[0]['iterated_rows'],
                         client_ms_median=round(med('client_ms'), 3), server_requests_median=req,
                         server_ms_median=round(med('server_us_sum') / 1000, 3),
                         server_fraction_median=round(med('server_fraction'), 4),
                         server_us_per_request=round(med('server_us_sum') / req, 1) if req else None,
                         client_ms_per_request=round(med('client_ms') / req, 4) if req else None))
    C.csv_new(OUT / 'trace_summary.csv', summ)
    C.write_new(OUT / 'trace_summary.json', dict(status='F3_TRACE_COMPLETE', created_utc=C.now_iso(), reps=REPS,
                                                 sessions_matched=sum(r['server_requests'] for r in raw),
                                                 sessions_in_window=len(sess), rows=summ,
                                                 note='tracing adds overhead; values decompose time, they are not reference latencies'))
    C.log(f'PASS trace: {len(raw)} executions, {sum(r["server_requests"] for r in raw)} traced requests matched of {len(sess)}')


if __name__ == '__main__':
    main()
