"""TPCx-IoT adapter: frozen Stage 4-6 artefacts + the EXECUTED campaign parameters (not the illustrative
10 stations x 200 sensors scenario of the old Stage 7). Read work per logical read is averaged over the frozen,
deterministic query schedule (make_probe/schedule_by_step); row counts come from the deterministic event
generator (no timing). Deployment feasibility = the frozen Stage 8 admissibility rule (one canonical source,
at most one TS3 projection, TS3 never canonical)."""
from __future__ import annotations

import json
import sys
from collections import Counter

from ..model import AccessStep, CandidateSpec, Column, Operation, QueryPlan, Workload, WritePlan
from ..provenance import REPO, sha

SNAP = REPO / 'versao_ajustada/tpcx_iot_benchmark/frozen_design'
W = dict(stations=2, sensors=3, seed_steps=130, bench_steps=500, read_operations=120, tick_ms=1000, pad_bytes=900)


def _imp():
    if str(SNAP) not in sys.path:
        sys.path.insert(0, str(SNAP))
    from methodology.tpcx_iot.stage5.runtime import bind_read_requests
    from methodology.tpcx_iot.stage9.experiment import events
    from methodology.tpcx_iot.stage91.experiment import expected_rows_from_watermark, make_probe, schedule_by_step
    from methodology.tpcx_iot.stage8.consolidator import digest
    return bind_read_requests, events, expected_rows_from_watermark, make_probe, schedule_by_step, digest


def build():
    bind, events, expected, make_probe, schedule, digest = _imp()
    specs = json.loads((SNAP / 'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json').read_text())
    plans = {p['candidate_id']: p for p in json.loads((SNAP / 'outputs/tpcx_iot_stage5/20_queryplans.json').read_text())}
    wplans = {p['candidate_id']: p for p in json.loads((SNAP / 'outputs/tpcx_iot_stage6/30_writeplans.json').read_text())}
    evs = events(stations=W['stations'], sensors=W['sensors'], steps=W['seed_steps'] + W['bench_steps'], tick_ms=W['tick_ms'], pad_bytes=W['pad_bytes'])
    widths = {'driver_system_id': 5.0, 'sensor_name': 10.0, 'padding': float(W['pad_bytes'])}
    stations = [f'ST{x:03}' for x in range(W['stations'])]
    # frozen read schedule of one execution (identical for every deployment)
    jobs = schedule(seed_steps=W['seed_steps'], bench_steps=W['bench_steps'], reads=W['read_operations'])
    probes = [make_probe(W['seed_steps'] + i, j, sensors=W['sensors'], tick_ms=W['tick_ms']) for i, js in sorted(jobs.items()) for j in js]
    rows_per_probe = [len(expected(p, stations=W['stations'], tick_ms=W['tick_ms'], pad_bytes=W['pad_bytes'])) for p in probes]
    cands, qplans, writes = [], {'Q_READ_2_WINDOWS': []}, {}
    for s in specs:
        cid = s['candidate_id']; b = s['bucket_seconds']
        cols = tuple(Column(n, t, widths.get(n)) for n, t in s['columns'].items())
        def pkey(e):
            vals = []
            for k in s['partition_key']:
                if k in e:
                    vals.append(e[k])
                else:   # time-bucket key column
                    vals.append((e['event_ts'] // (b * 1000)) * (b * 1000))
            return tuple(vals)
        part = Counter(pkey(e) for e in evs)
        role = wplans[cid]['role']
        tags = ('table:' + cid, f'bucket:{b or "none"}', 'raw') + (('projection',) if role == 'materialized_sensor_projection' else ('canonical:' + cid,))
        cands.append(CandidateSpec(cid, s['family'], cols, tuple(s['partition_key']), tuple(s['clustering_key']), float(len(evs)),
                                   float(len(part)), float(max(part.values())), float(len(evs)),
                                   canonical_source_required=role == 'materialized_sensor_projection', tags=tags))
        p = plans[cid]
        reqs = [len(bind(p, sensor_name=pr['sensor_name'], recent=pr['recent'], historical=pr['historical'], stations=stations,
                         complete_station_roster=True, roster_provenance='FULL_SYNTHETIC_GENERATOR_STATION_ROSTER')) for pr in probes]
        n = len(probes)
        select_cols = tuple(c.strip() for c in p['read_cql'].split('SELECT', 1)[1].split('FROM', 1)[0].split(','))
        step = AccessStep(cid, sum(reqs) / n, sum(reqs) / n, sum(rows_per_probe) / n, select_cols)
        qplans['Q_READ_2_WINDOWS'].append(QueryPlan('Q_READ_2_WINDOWS', p['plan_id'], (cid,), (step,), logical_operands=2.0,
                                                    runtime_guard='; '.join(p['read_preconditions']),
                                                    assumptions=(f'mean over the {n} frozen probes of one execution',),
                                                    provenance='tpcx stage5 20_queryplans.json + stage5.runtime.bind_read_requests'))
        writes[('Q_INGEST', cid)] = [WritePlan('Q_INGEST', cid, 1.0, 1.0, 0.0, 0.0, tuple(wplans[cid]['column_order']),
                                               assumptions=('one INSERT per event per table (frozen WritePlan)',),
                                               provenance='tpcx stage6 30_writeplans.json')]
    events_per_run = W['stations'] * W['sensors'] * (W['seed_steps'] + W['bench_steps'])
    wl = Workload((Operation('Q_READ_2_WINDOWS', 'read', float(W['read_operations'])), Operation('Q_INGEST', 'write', float(events_per_run))),
                  'FROZEN_WORKLOAD_PARAMETERS (120 reads and 3780 ingested events per execution)', 'PER_EXECUTION')
    roles = {c: wplans[c]['role'] for c in wplans}

    def enumerate_fn(est):
        can = sorted(c for c, r in roles.items() if r == 'canonical_series_table')
        proj = sorted(c for c, r in roles.items() if r == 'materialized_sensor_projection')
        return [frozenset({c} | ({p} if p else set())) for c in can for p in [None] + proj]
    tagmap = {c.candidate_id: c.tags for c in cands}
    scenario = {'benchmark': 'TPCx-IoT (research adaptation)', **W, 'events_per_execution': events_per_run,
                'reads_per_execution': W['read_operations'], 'read_threads': 2, 'probes_hash': sha_list([str((p['sensor_name'], p['recent'], p['historical'])) for p in probes]),
                'mean_rows_per_logical_read': sum(rows_per_probe) / len(rows_per_probe),
                'executed_stage9_scenario': 'identical to the V2 campaign (tpcx_v1 WORKLOAD)', 'replication_factor': 1,
                'old_stage7_scenario_NOT_USED': '10 stations x 200 sensors, 60 s, 1 day, RF=2'}
    return {'candidates': cands, 'plans_by_query': qplans, 'writes': writes, 'workload': wl, 'scenario': scenario,
            'enumerate_fn': enumerate_fn, 'deployments': None,
            'deployment_tags': lambda d: set().union(*(tagmap[c] for c in d)) | {'family:' + '+'.join(sorted(d))},
            'id_fn': lambda d: 'h_' + digest(sorted(d))[:12],
            'input_hashes': {str(p.relative_to(REPO)): sha(p) for p in (SNAP / 'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json',
                                                                       SNAP / 'outputs/tpcx_iot_stage5/20_queryplans.json',
                                                                       SNAP / 'outputs/tpcx_iot_stage6/30_writeplans.json',
                                                                       SNAP / 'methodology/tpcx_iot/stage9/experiment.py')}}


def sha_list(items):
    import hashlib
    return hashlib.sha256('\n'.join(items).encode()).hexdigest()
