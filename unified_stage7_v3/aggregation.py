"""Deployment-level aggregation: route choice, coverage/feasibility checks and the PhysicalWorkVector."""
from __future__ import annotations

from .model import Footprint, PhysicalWorkVector, ReadWork, WriteWork


def choose_route(plans, deployment, est):
    """Universal route rule: among valid plans whose tables are all in the deployment, lexicographic minimum of
    (read_cql_requests, read_logical_rows, read_logical_bytes, plan_id). Unknown bytes sort last."""
    ok = [p for p in plans if p.valid and set(p.candidate_ids) <= deployment]
    if not ok:
        return None, None
    scored = []
    for p in ok:
        rw, _ = est.read(p)
        scored.append(((rw.cql_requests, rw.logical_rows, float('inf') if rw.logical_bytes is None else rw.logical_bytes, p.plan_id), p, rw))
    scored.sort(key=lambda x: x[0])
    return scored[0][1], scored[0][2]


def deployment_vector(deployment, plans_by_query, writes, workload, est, support_fn=None):
    """Returns (PhysicalWorkVector, routes, problems). Problems non-empty -> deployment infeasible.

    support_fn(deployment) -> ({write_op: (maintenance_read_requests, maintenance_rows_read)}, used_tables, problems)
    resolves maintenance (support) reads shared by all tables a write maintains, once per (write, entity)."""
    deployment = frozenset(deployment)
    problems, routes, warns = [], {}, []
    reads = [o for o in workload.operations if o.kind == 'read']
    fsum = sum(o.frequency for o in reads) or 1.0
    acc = dict(req=0.0, parts=0.0, rows=0.0, bytes=0.0, fan=0.0)
    for o in reads:
        p, rw = choose_route(plans_by_query.get(o.op_id, []), deployment, est)
        if p is None:
            problems.append(f'no valid route for {o.op_id}'); continue
        routes[o.op_id] = p.plan_id
        f = o.frequency / fsum
        acc['req'] += f * rw.cql_requests; acc['parts'] += f * rw.partitions_touched; acc['rows'] += f * rw.logical_rows
        acc['fan'] += f * rw.bucket_or_partition_fanout
        acc['bytes'] = None if (acc['bytes'] is None or rw.logical_bytes is None) else acc['bytes'] + f * rw.logical_bytes
    wacc = dict(mut=0.0, rows=0.0, mreq=0.0, mrows=0.0, bytes=0.0)
    for o in (x for x in workload.operations if x.kind == 'write'):
        for cid in sorted(deployment):
            for wp in writes.get((o.op_id, cid), []):
                if not wp.maintainable:
                    problems.append(f'{cid} not maintainable by {o.op_id}'); continue
                ww, w2 = est.write(wp); warns += list(w2)
                wacc['mut'] += o.frequency * ww.cql_mutations; wacc['rows'] += o.frequency * ww.rows_touched
                wacc['mreq'] += o.frequency * ww.maintenance_read_requests; wacc['mrows'] += o.frequency * ww.maintenance_rows_read
                wacc['bytes'] = None if (wacc['bytes'] is None or ww.logical_bytes_written is None) else wacc['bytes'] + o.frequency * ww.logical_bytes_written
    sup_used = set()
    if support_fn is not None:
        sup, sup_used, sp = support_fn(deployment)
        problems += sp
        for o in (x for x in workload.operations if x.kind == 'write'):
            r, rr = sup.get(o.op_id, (0.0, 0.0))
            wacc['mreq'] += o.frequency * r; wacc['mrows'] += o.frequency * rr
    st, parts, maxr, maxb, dup = 0.0, 0.0, 0.0, 0.0, 0.0
    for cid in sorted(deployment):
        fp, w3 = est.footprint(cid); warns += list(w3)
        st = None if (st is None or fp.logical_storage_bytes is None) else st + fp.logical_storage_bytes
        parts += fp.physical_partition_count; maxr = max(maxr, fp.max_partition_rows)
        maxb = None if (maxb is None or fp.max_partition_bytes is None) else max(maxb, fp.max_partition_bytes)
        dup += fp.duplication_ratio
    # universal structural admissibility (D-V3-007): a canonical source must be present and every derived table
    # must serve at least one chosen read route (no dead tables). No cost is involved.
    canon = [c for c in deployment if not est.cand[c].canonical_source_required]
    if not canon:
        problems.append('no canonical source table')
    used = set(sup_used)
    for o in reads:
        if o.op_id in routes:
            used |= set(next(p for p in plans_by_query[o.op_id] if p.plan_id == routes[o.op_id]).candidate_ids)
    for c in sorted(deployment):
        if est.cand[c].canonical_source_required and c not in used:
            problems.append(f'dead table {c}: serves no chosen read route and no maintenance support read')
    vec = PhysicalWorkVector(
        read=ReadWork(acc['req'], acc['parts'], acc['rows'], acc['bytes'], acc['fan']),
        write=WriteWork(wacc['mut'], wacc['rows'], wacc['mreq'], wacc['mrows'], wacc['bytes']),
        footprint=Footprint(st, parts, maxr, maxb, dup), warnings=tuple(sorted(set(warns))))
    return vec, routes, problems
