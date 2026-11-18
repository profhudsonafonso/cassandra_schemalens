"""RUBiS adapter (SchemaLens V9 design space) for the Unified Stage 7 V3.

* Candidates: the 24 canonical tables of V9 E13 (CANONICAL_WITH_COVER.json) - pre-benchmark design artefact.
* QueryPlans / WritePlans: V9 automatic route synthesis (schemalens_v9.routes.read_options / write_needs), which is
  cost-free; the legacy NNLS model (schemalens_v9.cost, E14 COST_MODEL.json) is never imported or read.
* Scenario = the executed campaign (D-V3-003): the frozen cohort state that every deployment materialised in V2
  (versao_ajustada/rubis_v9/frozen/cohort_source.pkl: cohort + closure users). Statistics are recomputed in the
  E04 format on that state, with relationship fan-outs restricted to the frozen parameter domain of the histories
  (E05 parameter_domain), and injected into routes.read_options instead of the full-scale E04/E02 statistics.
* Table rows / partitions / widest partition are exact on the cohort state (schemalens_v9.phase5.gen_rows).
* Frequencies: E05 statement_frequency_per_request; aggregation over the 4 mixes = arithmetic mean (D-V3-009).
* Search: exhaustive enumeration of all subsets of the non-mandatory tables (no ILP objective, no a/b/c).
"""
from __future__ import annotations

import hashlib
import itertools
import json
import pickle
from collections import Counter, defaultdict

from ..model import AccessStep, CandidateSpec, Column, Operation, QueryPlan, Workload, WritePlan
from ..provenance import REPO, sha

RUN = REPO / 'runs/v9_generation_001'
CANON_FILE = RUN / 'stages/E13_route_synthesis/CANONICAL_WITH_COVER.json'
WORKLOAD_FILE = RUN / 'stages/E05_workload/WORKLOAD.json'
STATS_FILE = RUN / 'stages/E04_observed_stats/STATS.json'          # read only for its structure/labels
COHORT = REPO / 'versao_ajustada/rubis_v9/frozen/cohort_source.pkl'
MIXES = ('browsing', 'bidding', 'update10x', 'update100x')
SINCE_LABEL = 'items.end_date >= 2010-01-01 (since fixo da carga)'


def _dist(counter, parents):
    from schemalens_v9.phase1b import _dist as d
    return d(counter, parents)


def cohort_stats(state, domain, template):
    """E04-format statistics on the executed cohort state; fan-outs over the histories' parameter domain."""
    from schemalens_v9.schema_map import FK
    from datetime import datetime
    counts = {e: len(v) for e, v in state.items()}
    rels = {}
    for label, v in template['relationships'].items():
        src, attr = v['fk'].split('.')
        target = label.split('-> ')[1].split(' ')[0]
        dom = [i for i in domain.get(target, []) if i in state[target]] or sorted(state[target])
        ctr = Counter(r.get(attr) for r in state[src].values() if r.get(attr) in set(dom))
        rels[label] = {'fk': v['fk'], 'null_fk_rows': sum(1 for r in state[src].values() if r.get(attr) is None),
                       'fanout_per_target': _dist(ctr, len(dom)), 'parent_domain': 'E05 parameter_domain' if domain.get(target) else 'all'}
    region_of = {i: u.get('region_id') for i, u in state['users'].items()}
    dom_r, dom_c = set(domain['regions']), set(domain['categories'])
    comp = Counter((region_of.get(it.get('seller_id')), it.get('category_id')) for it in state['items'].values())
    comp = Counter({k: v for k, v in comp.items() if k[0] in dom_r and k[1] in dom_c})
    ends = [it.get('end_date') for it in state['items'].values()]
    sel = sum(1 for e in ends if e is not None and e >= datetime(2010, 1, 1)) / len(ends)
    return ({'entity_counts': counts, 'relationships': rels,
             'composite_partitions': {'items por (seller.region_id, category_id)': _dist(comp, len(dom_r) * len(dom_c))},
             'range_selectivity': {SINCE_LABEL: round(sel, 6)}, 'source': 'cohort state (V2 frozen cohort_source.pkl)',
             'estimated_row_bytes_model': template.get('estimated_row_bytes_model'), 'row_bytes_basis': template.get('row_bytes_basis')},
            counts)


def install_campaign_stats():
    """Campaign-scale statistics (cohort state, domain-restricted fan-outs) injected into the V9 route synthesis
    (schemalens_v9.phase2.stats / routes.stats) in place of the full-scale E04/E02 statistics."""
    import schemalens_v9.phase2 as F
    import schemalens_v9.routes as R
    from schemalens_v9.schema_map import from_full_row
    wl = json.loads(WORKLOAD_FILE.read_text())
    template = json.loads(STATS_FILE.read_text())
    raw = pickle.loads(COHORT.read_bytes())['rubis_full_rows']
    state = {e: {i: from_full_row(e, r) for i, r in rows.items()} for e, rows in raw.items()}
    domain = wl['parameter_domain']['domain']
    st, counts = cohort_stats(state, domain, template)
    F.stats = R.stats = lambda: (st, counts)
    return state, st, counts, domain


def plan_id(q, o):
    return f"q{q}:{o['main']}" + ''.join('+' + l[3] for l in o['lookups'])


def build():
    import schemalens_v9.phase2 as F
    import schemalens_v9.routes as R
    from schemalens_v9.contract import CONTRACT, READ_IDS, WRITE_IDS
    from schemalens_v9.phase5 import gen_rows
    from schemalens_v9.oracle_v9 import _nav
    C = json.loads(CANON_FILE.read_text())['tables']
    wl = json.loads(WORKLOAD_FILE.read_text())
    state, st, counts, domain = install_campaign_stats()
    by = {t['canonical_id']: t for t in C}

    def width(ent, ref):
        e, a, _ = F.resolve(ent, ref)
        vals = [_nav(state, ent, inst, ref)[0] for inst in list(state[ent].values())[:5000]]
        vals = [v for v in vals if isinstance(v, str)]
        return sum(len(v.encode()) for v in vals) / len(vals) if vals else None

    cands, tagmap = [], {}
    inserted = {CONTRACT[w]['entity'] for w in WRITE_IDS if CONTRACT[w]['op'] == 'INSERT'}
    complete_copy = {}
    for w in WRITE_IDS:
        cw = CONTRACT[w]
        if cw['op'] == 'INSERT':
            complete_copy[w] = [t['canonical_id'] for t in C if t['row_entity'] == cw['entity'] and set(cw['values']) <= R._avail(t) | {'id'}]
    canonical_ids = set(itertools.chain.from_iterable(complete_copy.values()))
    for t in C:
        ent = t['row_entity']
        refs = t['partition'] + [r for r, _ in t['clustering']] + t['columns']
        cols = []
        for r in refs:
            ty = F.coltype(ent, r)
            cols.append(Column(F.colname(r), ty, width(ent, r) if ty in ('text', 'varchar', 'ascii') else None))
        rows = gen_rows(t, state)
        part = Counter(tuple(x[k] for k in t['partition']) for x in rows)
        tags = ('table:' + t['canonical_id'], 'entity:' + ent) + tuple('family:' + f for f in t['families']) \
            + (('canonical:' + ent,) if t['canonical_id'] in canonical_ids else ('projection',)) \
            + (('variant:' + ent + ':' + '+'.join(t['partition'])),)
        tagmap[t['canonical_id']] = tags
        cands.append(CandidateSpec(t['canonical_id'], '+'.join(t['families']), tuple(cols), tuple(t['partition']),
                                   tuple(r for r, _ in t['clustering']), float(len(rows)), float(len(part)),
                                   float(max(part.values()) if part else 0), float(len(state[ent])),
                                   canonical_source_required=t['canonical_id'] not in canonical_ids, tags=tags))
    colsof = {c.candidate_id: tuple(x.name for x in c.columns) for c in cands}
    plans = {}
    for q in READ_IDS:
        lst = []
        for o in R.read_options(q, C):
            lk = [l[3] for l in o['lookups']]
            steps = [AccessStep(o['main'], 1.0, 1.0, float(o['rows_main']), colsof[o['main']])]
            for l in lk:
                steps.append(AccessStep(l, float(o['rows_lookup_each']), float(o['rows_lookup_each']), float(o['rows_lookup_each']), colsof[l]))
            pid = plan_id(q, o)
            lst.append(QueryPlan(str(q), pid, tuple(dict.fromkeys([o['main']] + lk)), tuple(steps), 1.0,
                                 runtime_guard='relaxed_drop=' + ','.join(o['relaxed_drop']) if o['relaxed_drop'] else '',
                                 assumptions=('V9 route synthesis; expected rows on the cohort state, domain-restricted fan-outs',
                                              f"range_pushdown={o['range_pushdown']} limit_pushdown={o['limit_pushdown']}"),
                                 provenance='schemalens_v9.routes.read_options'))
        plans[str(q)] = lst
    writes = {}
    for w in WRITE_IDS:
        for t in C:
            n = R.write_needs(w, t)
            if not n['affected']:
                continue
            cid = t['canonical_id']
            writes[(str(w), cid)] = [WritePlan(str(w), cid, float(n.get('writes', 1)), float(n.get('writes', 1)), 0.0, 0.0, colsof[cid],
                                               maintainable=n['maintainable'], warnings=(() if n['maintainable'] else (n['reason'],)),
                                               provenance='schemalens_v9.routes.write_needs')]
    freq = wl['statement_frequency_per_request']
    mean_f = {s: sum(freq[m][s] for m in MIXES) / len(MIXES) for s in freq['bidding']}
    ops = tuple(Operation(str(q), 'read', mean_f[str(q)]) for q in READ_IDS) + tuple(Operation(str(w), 'write', mean_f[str(w)]) for w in WRITE_IDS)
    workload = Workload(ops, 'E05 statement_frequency_per_request', 'MEAN_OF_E05_MIXES (browsing, bidding, update10x, update100x; equal weight, D-V3-009)')

    def support_fn(dep):
        S = [by[c] for c in sorted(dep)]
        out, used, problems = {}, set(), []
        for w in WRITE_IDS:
            sup = defaultdict(set)
            for t in S:
                n = R.write_needs(w, t)
                if n['affected'] and n['maintainable']:
                    for e2, attrs in n['support'].items():
                        sup[e2] |= set(attrs)
            nreq = 0
            for e2, attrs in sorted(sup.items()):
                L = R._lookup_tables(S, e2, attrs)
                if L:
                    used.add(min(L, key=lambda x: (len(x['columns']), x['canonical_id']))['canonical_id']); nreq += 1
                    continue
                for a in sorted(attrs):
                    L = R._lookup_tables(S, e2, {a})
                    if not L:
                        problems.append(f'write {w}: no support table for {e2}.{a}'); continue
                    used.add(min(L, key=lambda x: (len(x['columns']), x['canonical_id']))['canonical_id']); nreq += 1
            out[str(w)] = (float(nreq), float(nreq))
        return out, used, problems

    def feasible_fn(dep):
        problems = []
        for w, full in complete_copy.items():          # D-V9-010: one complete copy of every inserted entity
            if not set(full) & set(dep):
                problems.append(f'write {w}: no complete copy of {CONTRACT[w]["entity"]}')
        for cid in sorted(dep):                          # V9 `supersedes` admissibility
            t = by[cid]
            for sup in t['supersedes']:
                for u in sorted(dep):
                    if u != cid and any(o['family'] == sup['family'] and o['statement'] == sup['statement'] for o in by[u]['origin_all']):
                        problems.append(f'{cid} supersedes {u}')
        return problems

    mandatory = set()
    for q, lst in plans.items():
        mandatory |= set.intersection(*(set(p.candidate_ids) for p in lst))
    optional = sorted(set(by) - mandatory)

    def enumerate_fn(est):
        for n in range(len(optional) + 1):
            for combo in itertools.combinations(optional, n):
                yield frozenset(mandatory | set(combo))
    scenario = {'benchmark': 'RUBiS (SchemaLens V9 design space)', 'state': 'V2 frozen cohort (cohort + closure users), identical to the measured campaign',
                'entity_counts': counts, 'cohort_sha256': sha(COHORT), 'parameter_domain': domain,
                'statistics_E04_format_on_cohort': st, 'frequency_source': 'E05', 'mixes': list(MIXES),
                'frequency_aggregation': 'MEAN_OF_E05_MIXES', 'mean_frequencies': mean_f,
                'mandatory_tables': sorted(mandatory), 'optional_tables': optional, 'search': f'exhaustive 2^{len(optional)} subsets',
                'old_stage7_scenario_NOT_USED': 'full-scale E04/E02 statistics (rubis_full: 2M items, 20M bids) + NNLS a/b/c'}
    return {'candidates': cands, 'plans_by_query': plans, 'writes': writes, 'workload': workload, 'scenario': scenario,
            'enumerate_fn': enumerate_fn, 'deployments': None, 'support_fn': support_fn, 'feasible_fn': feasible_fn,
            'deployment_tags': lambda d: set().union(*(tagmap[c] for c in d)),
            'id_fn': lambda d: 'V3R_' + hashlib.sha256(','.join(sorted(d)).encode()).hexdigest()[:10],
            'per_mix_frequencies': {m: freq[m] for m in MIXES},
            'input_hashes': {str(p.relative_to(REPO)): sha(p) for p in (CANON_FILE, WORKLOAD_FILE, STATS_FILE, COHORT)}}
