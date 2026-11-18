"""Canonical data model of the Unified Stage 7 V3 (identical schema for every benchmark).

Unknown quantities are represented explicitly (None + an 'UNKNOWN:<field>' warning) and are never
silently replaced by zero. Quantities must be finite and non-negative.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Optional


def _check(obj, name):
    for k, v in asdict(obj).items():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if not math.isfinite(v) or v < 0:
                raise ValueError(f'{name}.{k} must be finite and non-negative, got {v!r}')


# ---------------- canonical inputs produced by adapters (pure description, no cost semantics)
@dataclass(frozen=True)
class Column:
    name: str
    cql_type: str
    mean_width_bytes: Optional[float] = None      # required for variable-width types


@dataclass(frozen=True)
class CandidateSpec:
    candidate_id: str
    family: str                                   # label only: never used in any computation
    columns: tuple                                # tuple[Column]
    partition_key: tuple                          # column names
    clustering_key: tuple
    rows_total: float                             # rows stored for the campaign scenario
    partition_count: float
    max_partition_rows: float
    source_rows: float                            # logical rows of the canonical source entity (duplication denominator)
    canonical_source_required: bool
    tags: tuple = ()                              # structural diversity tags (canonical:, table:, bucket:, projection, aggregate)
    warnings: tuple = ()


@dataclass(frozen=True)
class AccessStep:
    candidate_id: str
    cql_requests: float                           # CQL statements issued per execution
    partitions_touched: float
    rows_read: float                              # rows read from the table per execution
    projected_columns: tuple                      # names of columns returned


@dataclass(frozen=True)
class QueryPlan:
    query_id: str
    plan_id: str
    candidate_ids: tuple                          # tables the route requires
    steps: tuple                                  # tuple[AccessStep]
    logical_operands: float                       # logical series/entities the query asks for (fan-out denominator)
    runtime_guard: str = ''
    valid: bool = True
    assumptions: tuple = ()
    warnings: tuple = ()
    provenance: str = ''


@dataclass(frozen=True)
class WritePlan:
    write_id: str
    candidate_id: str
    cql_mutations: float                          # per write execution
    rows_touched: float
    maintenance_read_requests: float
    maintenance_rows_read: float
    written_columns: tuple
    maintainable: bool = True
    assumptions: tuple = ()
    warnings: tuple = ()
    provenance: str = ''


@dataclass(frozen=True)
class Operation:
    op_id: str
    kind: str                                     # 'read' | 'write'
    frequency: float


@dataclass(frozen=True)
class Workload:
    operations: tuple                             # tuple[Operation]
    frequency_source: str
    aggregation_policy: str


# ---------------- outputs (identical schema everywhere)
@dataclass(frozen=True)
class ReadWork:
    cql_requests: float
    partitions_touched: float
    logical_rows: float
    logical_bytes: Optional[float]
    bucket_or_partition_fanout: float


@dataclass(frozen=True)
class WriteWork:
    cql_mutations: float
    rows_touched: float
    maintenance_read_requests: float
    maintenance_rows_read: float
    logical_bytes_written: Optional[float]


@dataclass(frozen=True)
class Footprint:
    logical_storage_bytes: Optional[float]
    physical_partition_count: float
    max_partition_rows: float
    max_partition_bytes: Optional[float]
    duplication_ratio: float


@dataclass(frozen=True)
class PhysicalWorkVector:
    read: ReadWork
    write: WriteWork
    footprint: Footprint
    warnings: tuple = field(default_factory=tuple)
    assumptions: tuple = field(default_factory=tuple)

    def axes(self) -> dict:
        return {'read_cql_requests': self.read.cql_requests, 'read_logical_rows': self.read.logical_rows,
                'read_logical_bytes': self.read.logical_bytes, 'write_cql_mutations': self.write.cql_mutations,
                'maintenance_read_requests': self.write.maintenance_read_requests,
                'logical_storage_bytes': self.footprint.logical_storage_bytes,
                'max_partition_bytes': self.footprint.max_partition_bytes,
                'max_partition_rows': self.footprint.max_partition_rows,
                'partition_count': self.footprint.physical_partition_count}


def validate(*objs):
    for o in objs:
        _check(o, type(o).__name__)
    return True
