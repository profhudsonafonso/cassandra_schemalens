"""Generic adapter: builds canonical objects from a plain JSON description (no benchmark-specific logic).
Used to show that identical canonical inputs give identical vectors whatever adapter produced them."""
from __future__ import annotations

from ..model import AccessStep, CandidateSpec, Column, QueryPlan, WritePlan


def candidate(d):
    return CandidateSpec(d['candidate_id'], d['family'], tuple(Column(c['name'], c['cql_type'], c.get('mean_width_bytes')) for c in d['columns']),
                         tuple(d['partition_key']), tuple(d['clustering_key']), float(d['rows_total']), float(d['partition_count']),
                         float(d['max_partition_rows']), float(d['source_rows']), bool(d['canonical_source_required']),
                         tuple(d.get('tags', ())), tuple(d.get('warnings', ())))


def plan(d):
    return QueryPlan(d['query_id'], d['plan_id'], tuple(d['candidate_ids']),
                     tuple(AccessStep(s['candidate_id'], float(s['cql_requests']), float(s['partitions_touched']), float(s['rows_read']),
                                      tuple(s['projected_columns'])) for s in d['steps']),
                     float(d['logical_operands']), d.get('runtime_guard', ''), bool(d.get('valid', True)))


def write(d):
    return WritePlan(d['write_id'], d['candidate_id'], float(d['cql_mutations']), float(d['rows_touched']),
                     float(d['maintenance_read_requests']), float(d['maintenance_rows_read']), tuple(d['written_columns']),
                     bool(d.get('maintainable', True)))
