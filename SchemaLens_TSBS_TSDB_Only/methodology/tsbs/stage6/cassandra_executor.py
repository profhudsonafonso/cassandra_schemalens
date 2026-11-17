"""Opt-in sequential execution adapter for Stage 6 CQL mutations.

It accepts a Cassandra-driver-like Session (prepare, execute, keyspace) and
is untested against live Cassandra. No driver is imported, so the rest of the
add-on needs only Python stdlib. This is NOT crash-safe, transactional,
concurrency-safe, or a TSBS-scale bulk loader. Use in an isolated keyspace.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Iterable


def _typed(value: Any, typ: str) -> Any:
    if typ == "date":
        return date.fromisoformat(value) if isinstance(value, str) else value
    if typ == "bigint":
        if type(value) is not int:
            raise ValueError("CQL bigint must be an int")
        return value
    if typ == "double":
        if type(value) not in (int, float):
            raise ValueError("CQL double must be numeric")
        return float(value)
    if typ == "text":
        if not isinstance(value, str):
            raise ValueError("CQL text must be a string")
        return value
    raise ValueError(f"Unsupported generated CQL type {typ}")


class ExperimentalCassandraExecutor:
    def __init__(self, session: Any, write_plans: list[dict], *, expected_keyspace: str):
        if not expected_keyspace or getattr(session, "keyspace", None) != expected_keyspace:
            raise ValueError("Use a Cassandra session bound to the explicit isolated keyspace")
        if not callable(getattr(session, "prepare", None)) or not callable(getattr(session, "execute", None)):
            raise TypeError("Expected a Cassandra Session with prepare() and execute()")
        self.session = session
        self.plans = {p["candidate_id"]: p for p in write_plans}
        if len(self.plans) != len(write_plans):
            raise ValueError("Duplicate candidate WritePlans")
        self.prepared: dict[str, Any] = {}

    def execute_mutations(self, mutations: Iterable[dict], *, allow_experimental_writes: bool = False) -> int:
        if allow_experimental_writes is not True:
            raise PermissionError("This is not crash-safe. Opt in with allow_experimental_writes=True")
        count = 0
        for change in mutations:
            plan = self.plans[change["candidate_id"]]
            if change["table_name"] != plan["table_name"]:
                raise ValueError("Mutation table differs from approved WritePlan")
            op = change["operation"]
            if op == "UPSERT":
                stmt, columns = plan["write_cql"], plan["column_order"]
            elif op == "DELETE":
                stmt, columns = plan["delete_cql"], plan["identity_columns"]
            else:
                raise ValueError(f"Unsupported operation {op}")
            if change["cql"] != stmt:
                raise ValueError("CQL differs from approved generated WritePlan")
            if len(change["parameters"]) != len(columns):
                raise ValueError("Invalid mutation binding cardinality")
            values = tuple(_typed(v, plan["column_types"][c])
                           for c, v in zip(columns, change["parameters"]))
            if stmt not in self.prepared:
                self.prepared[stmt] = self.session.prepare(stmt)
            self.session.execute(self.prepared[stmt], values)
            count += 1
        return count
