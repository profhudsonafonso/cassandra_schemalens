"""TSBS cpu-only adapter: frozen Stage 4-6 artefacts + the EXECUTED campaign (official Go generator data: 40 hosts,
600 s, 36 h; 96 frozen query instances, natural + aligned cohorts). NOT the old 100 hosts / 60 s / 3 days scenario.

Each frozen query instance is one read operation (uniform: frequency_source = UNSPECIFIED, aggregation =
UNIFORM_QUERY_CLASS, 6 instances per template). A plan is valid for an instance only if its runtime guard binds
(alignment); otherwise the universal route rule picks another valid plan of the deployment. Rows read per request
are counted on the in-memory reference materialization of the campaign data (no timing)."""
from __future__ import annotations

import json
import sys
from collections import Counter

from ..model import AccessStep, CandidateSpec, Column, Operation, QueryPlan, Workload, WritePlan
from ..provenance import REPO, sha

from ..layout import V2 as _V2

V2 = _V2['tsbs']
SNAP = V2 / 'frozen_design'
DATA = V2 / 'data/tsbs_cpu_only_cassandra_double.txt.gz'
INST = V2 / 'data/query_instances.json'
CANON = 'ts0_metric_raw'


def _imp():
    for p in (SNAP,):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    from methodology.tsbs.stage9.dataset import load_cpu_dataset
    from methodology.tsbs.stage6.materializer import ReferenceMaterializer
    from methodology.tsbs.stage9.offline import IndexedReferenceFetcher
    from methodology.tsbs.stage5.runtime import bind_read_requests
    from methodology.tsbs.workload import METRICS
    return load_cpu_dataset, ReferenceMaterializer, IndexedReferenceFetcher, bind_read_requests, METRICS


def _select_cols(cql):
    return tuple(c.strip() for c in cql.split('SELECT', 1)[1].split('FROM', 1)[0].split(','))


def build():
    load, RM, Fetch, bind, METRICS = _imp()
    specs = json.loads((SNAP / 'outputs/tsbs_stage4/10_cassandra_candidate_specs.json').read_text())
    plans = json.loads((SNAP / 'outputs/tsbs_stage5/15_query_plans.json').read_text())
    writes = json.loads((SNAP / 'outputs/tsbs_stage6/18_write_plans.json').read_text())
    instances = json.loads(INST.read_text())
    ds = load(DATA, max_observations=50000)
    cids = [s['candidate_id'] for s in specs]
    m = RM(writes, list(METRICS), candidate_ids=cids)
    muts = m.apply(ds['events'])
    fetch = Fetch(m)
    host_w = sum(len(h) for h in ds['hostnames']) / len(ds['hostnames'])
    metric_w = sum(len(x) for x in METRICS) / len(METRICS)
    widths = {'hostname': host_w, 'metric_name': metric_w}
    wp = {w['candidate_id']: w for w in writes}
    mut_count = Counter(x['candidate_id'] for x in muts)
    cands, tagmap = [], {}
    for s in specs:
        cid = s['candidate_id']
        types = wp[cid]['column_types']
        cols = tuple(Column(n, t, widths.get(n)) for n, t in types.items())
        rows = list(m.rows[cid].values())
        part = Counter(tuple(r[k] for k in s['partition_key']) for r in rows)
        agg = s['family'] in ('TS5', 'TS6')
        tags = ('table:' + cid, 'aggregate' if agg else 'raw') + (('canonical:' + cid,) if cid == CANON else ('projection',))
        if 'bucket_day' in s['partition_key']:
            tags += ('bucket:' + (s.get('variant') or 'day'),)
        tagmap[cid] = tags
        cands.append(CandidateSpec(cid, s['family'], cols, tuple(s['partition_key']), tuple(s['clustering_key']), float(len(rows)),
                                   float(len(part)), float(max(part.values()) if part else 0), float(ds['metric_cells']),
                                   canonical_source_required=cid != CANON, tags=tags))
    by_q = {}
    for p in plans:
        by_q.setdefault(p['query_id'], []).append(p)
    qplans, ops = {}, []
    for inst in instances:
        op = inst['instance_key']
        ops.append(Operation(op, 'read', 1.0))
        lst = []
        for p in by_q[inst['query_id']]:
            try:
                reqs = bind(p, **inst['params'])
                valid, guard = True, p.get('alignment_guard') or ''
            except ValueError as exc:
                reqs, valid, guard = [], False, f'RUNTIME_GUARD_FAILS: {exc}'
            rows = float(sum(len(fetch(r)) for r in reqs))
            parts = float(len({(r['candidate_id'], tuple(r['parameters'][:len(next(s for s in specs if s['candidate_id'] == r['candidate_id'])['partition_key'])])) for r in reqs}))
            step = AccessStep(p['candidate_id'], float(len(reqs)), parts, rows, _select_cols(p['read_cql']))
            lst.append(QueryPlan(op, p['plan_id'], (p['candidate_id'],), (step,), logical_operands=float(max(1, len(inst['params'].get('hostnames', [])))),
                                 runtime_guard=guard, valid=valid, assumptions=('rows counted on the in-memory reference materialization of the campaign data',),
                                 provenance='tsbs stage5 15_query_plans.json + stage5.runtime.bind_read_requests'))
        qplans[op] = lst
    wplans = {('load', c.candidate_id): [WritePlan('load', c.candidate_id, float(mut_count[c.candidate_id]), c.rows_total, 0.0, 0.0,
                                                   tuple(x.name for x in c.columns),
                                                   assumptions=('static campaign load via the Stage 6 reference materializer; derived tables recomputed in memory (no Cassandra read)',),
                                                   provenance='tsbs stage6 18_write_plans.json + ReferenceMaterializer')] for c in cands}
    ops.append(Operation('load', 'write', 1.0))
    wl = Workload(tuple(ops), 'UNSPECIFIED', 'UNIFORM_QUERY_CLASS (6 frozen instances per template: 3 natural + 3 aligned)')
    optional = [c for c in cids if c != CANON]

    def enumerate_fn(est):
        import itertools
        out = []
        for n in range(len(optional) + 1):
            for combo in itertools.combinations(optional, n):
                out.append(frozenset((CANON,) + combo))
        return out
    scenario = {'benchmark': 'TSBS cpu-only', 'generator': 'tsbs_generate_data commit 8323e59c, --scale=40 --log-interval=600s, 36 h, seed 20261117',
                'hosts': len(ds['hostnames']), 'observations': ds['observations'], 'metric_cells': ds['metric_cells'],
                'instances': len(instances), 'cohorts': ['natural', 'aligned'], 'instances_sha256': sha(INST), 'data_sha256': sha(DATA),
                'executed_stage9_scenario': 'identical to the V2 campaign (same data file and instance file)',
                'old_stage7_scenario_NOT_USED': '100 hosts, 60 s, 3 days', 'text_widths_bytes': widths}
    return {'candidates': cands, 'plans_by_query': qplans, 'writes': wplans, 'workload': wl, 'scenario': scenario,
            'enumerate_fn': enumerate_fn, 'deployments': None,
            'deployment_tags': lambda d: set().union(*(tagmap[c] for c in d)) | {'family:' + '+'.join(sorted(d))},
            'id_fn': lambda d: 'V3TSBS_' + '+'.join(sorted(x.replace('_', '') for x in d)),
            'input_hashes': {str(p.relative_to(REPO)): sha(p) for p in (SNAP / 'outputs/tsbs_stage4/10_cassandra_candidate_specs.json',
                                                                       SNAP / 'outputs/tsbs_stage5/15_query_plans.json',
                                                                       SNAP / 'outputs/tsbs_stage6/18_write_plans.json', DATA, INST)}}
