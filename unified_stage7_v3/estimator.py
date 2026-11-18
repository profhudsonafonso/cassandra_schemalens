"""UnifiedStage7Estimator: CandidateSpec + QueryPlan + WritePlan + scenario -> physical work.

No scalarisation, no latency, no family factor: the family label is never read.
"""
from __future__ import annotations

from .logical_bytes import row_width
from .model import Footprint, ReadWork, WriteWork, validate


class UnifiedStage7Estimator:
    def __init__(self, candidates):
        self.cand = {c.candidate_id: c for c in candidates}
        if len(self.cand) != len(candidates):
            raise ValueError('duplicate candidate ids')
        self._rcache = {}

    def footprint(self, cid) -> tuple:
        c = self.cand[cid]
        w, warns = row_width(c.columns)
        storage = None if w is None else c.rows_total * w
        maxp = None if w is None else c.max_partition_rows * w
        dup = c.rows_total / c.source_rows if c.source_rows else 0.0
        fp = Footprint(logical_storage_bytes=storage, physical_partition_count=float(c.partition_count),
                       max_partition_rows=float(c.max_partition_rows), max_partition_bytes=maxp, duplication_ratio=dup)
        validate(fp)
        return fp, tuple(warns) + tuple(c.warnings)

    def read(self, plan) -> tuple:
        key = (plan.query_id, plan.plan_id)
        if key not in self._rcache:
            self._rcache[key] = self._read(plan)
        return self._rcache[key]

    def _read(self, plan) -> tuple:
        req = parts = rows = 0.0
        byt, warns = 0.0, list(plan.warnings)
        for s in plan.steps:
            if s.candidate_id not in self.cand:
                raise ValueError(f'{plan.plan_id}: step on unknown candidate {s.candidate_id}')
            req += s.cql_requests; parts += s.partitions_touched; rows += s.rows_read
            w, ww = row_width(self.cand[s.candidate_id].columns, s.projected_columns)
            warns += ww
            byt = None if (byt is None or w is None) else byt + s.rows_read * w
        fan = parts / plan.logical_operands if plan.logical_operands else parts
        rw = ReadWork(cql_requests=req, partitions_touched=parts, logical_rows=rows, logical_bytes=byt,
                      bucket_or_partition_fanout=fan)
        validate(rw)
        return rw, tuple(warns)

    def write(self, wp) -> tuple:
        w, warns = row_width(self.cand[wp.candidate_id].columns, wp.written_columns)
        ww = WriteWork(cql_mutations=wp.cql_mutations, rows_touched=wp.rows_touched,
                       maintenance_read_requests=wp.maintenance_read_requests,
                       maintenance_rows_read=wp.maintenance_rows_read,
                       logical_bytes_written=None if w is None else wp.rows_touched * w)
        validate(ww)
        return ww, tuple(warns) + tuple(wp.warnings)
