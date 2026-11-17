#!/usr/bin/env python3
"""Fase 4, E16 (EMENDA_05 §4): tracing of P1, windows 1 and 7 days, in the E9 and e9r2 keyspaces.

    python fase4_trace.py run        # writes experiments/rev3_fase2/e16/ (refuses to overwrite)

Fixes of EMENDA_05 §1 relative to fase3_trace.py: system_traces started_at is naive UTC; every session is assigned to at
most one execution (the one whose interval contains the session start); executions are separated by 20 ms.
"""
from __future__ import annotations

import bisect
import csv
import datetime as dt
import math
import statistics
import sys
import time

import fase4_core  # noqa: F401  (registers Fase 3 + Fase 4 campaigns and the P1 case subset)
import fase2_core as C

OUT = C.F2 / 'e16'
SAMPLE = [('e9', 'p1'), ('e9r2', 'p1')]
QUERIES, WINDOWS = ('Q2', 'Q12'), (1, 7)
VARIANTS = (C.U, C.D)
REPS = 30
GAP = 0.020


def trace_probability(p):
    if p not in (0, 1):
        raise ValueError('only 0 or 1')
    C._docker(['exec', C.S3.CONTAINER, 'nodetool', 'settraceprobability', str(p)])
    got = C._docker(['exec', C.S3.CONTAINER, 'nodetool', 'gettraceprobability'])
    C.log('trace probability ->', got.strip())


def utc_ts(x):
    return x.replace(tzinfo=dt.timezone.utc).timestamp() if x.tzinfo is None else x.timestamp()


def p95(xs):
    s = sorted(xs)
    return s[max(0, math.ceil(0.95 * len(s)) - 1)]


def main():
    if sys.argv[1:] != ['run']:
        sys.exit(__doc__)
    OUT.mkdir(parents=True, exist_ok=False)
    raw = []
    t_begin = time.time()
    try:
        trace_probability(1)
        for camp, ds in SAMPLE:
            info, sensors, days, oracle, compare, cases, expected = C.oracle_bundle(ds)
            chosen = [q for q in cases if q['query_id'] in QUERIES and q['s1_window_days'] in WINDOWS]
            cluster, session = C.connect()
            try:
                execute = C.make_executor(session, sensors, days, oracle)
                for q in chosen:
                    for label in VARIANTS:
                        session.set_keyspace(C.ks_name(camp, ds, 'sst', label))
                        for rep in range(1, REPS + 1):
                            time.sleep(GAP)
                            t0 = time.time()
                            p0 = time.perf_counter_ns()
                            got, tr = execute(q, label)
                            ms = (time.perf_counter_ns() - p0) / 1e6
                            t1 = time.time()
                            if not compare(got, expected[q['s1_case_id']]):
                                raise AssertionError('semantic mismatch under tracing')
                            raw.append(dict(campaign=camp, dataset=ds, case_id=q['s1_case_id'], variant=C.SHORT[label],
                                            rep=rep, start=t0, end=t1, client_ms=ms, execute_calls=tr['execute_calls'],
                                            iterated_rows=tr['iterated_rows']))
                        C.log(f'{camp} {ds} {q["s1_case_id"]} {C.SHORT[label]}: {REPS} traced executions')
            finally:
                cluster.shutdown()
    finally:
        trace_probability(0)
    time.sleep(15)  # traces are written asynchronously
    cluster, session = C.connect()
    try:
        sess = sorted((utc_ts(r.started_at), r.duration) for r in
                      session.execute('SELECT started_at, duration FROM system_traces.sessions')
                      if r.started_at and r.duration is not None and utc_ts(r.started_at) >= t_begin - 5)
    finally:
        cluster.shutdown()
    with (OUT / 'system_traces_sessions.csv').open('x', newline='') as f:
        w = csv.writer(f)
        w.writerow(['started_utc', 'duration_us'])
        w.writerows(sess)
    raw.sort(key=lambda r: r['start'])
    starts = [r['start'] for r in raw]
    for r in raw:
        r['server_requests'], r['server_us_sum'] = 0, 0
    unassigned = 0
    for t, dur in sess:
        i = bisect.bisect_right(starts, t + 0.001) - 1  # last execution starting before the session (1 ms clock slack)
        if i >= 0 and t <= raw[i]['end'] + 0.001:
            raw[i]['server_requests'] += 1
            raw[i]['server_us_sum'] += dur
        else:
            unassigned += 1
    for r in raw:
        r['server_fraction'] = round(r['server_us_sum'] / 1000 / r['client_ms'], 4) if r['client_ms'] else None
    with (OUT / 'trace_raw.csv').open('x', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(raw[0]))
        w.writeheader()
        w.writerows(raw)
    groups = {}
    for r in raw:
        groups.setdefault((r['campaign'], r['dataset'], r['case_id'], r['variant']), []).append(r)
    rows = []
    for (camp, ds, case, v), rr in sorted(groups.items()):
        med = lambda k: statistics.median(x[k] for x in rr)
        req = med('server_requests')
        rows.append(dict(dataset_campaign=f'{camp}/{ds}', case_id=case, variant=v.upper(), calls=rr[0]['execute_calls'],
                         rows=rr[0]['iterated_rows'], client_ms_median=round(med('client_ms'), 3),
                         client_ms_p95=round(p95([x['client_ms'] for x in rr]), 3), server_requests_median=req,
                         server_ms_median=round(med('server_us_sum') / 1000, 3),
                         server_fraction_median=round(med('server_fraction'), 4),
                         server_us_per_request=round(med('server_us_sum') / req, 1) if req else None))
    matched = sum(r['server_requests'] for r in raw)
    C.write_new(OUT / 'trace_summary.json', dict(status='F4_TRACE_COMPLETE', created_utc=C.now_iso(), reps=REPS,
                                                 sessions=len(sess), matched=matched, unassigned=unassigned,
                                                 note='tracing adds overhead; values decompose time, not reference latencies',
                                                 rows=rows))
    if matched == 0:
        raise SystemExit('FAIL trace: 0 sessions matched (EMENDA_05 forbids a silent PASS)')
    C.log(f'PASS trace: {len(raw)} executions, {matched} sessions matched, {unassigned} unassigned of {len(sess)}')


if __name__ == '__main__':
    main()
