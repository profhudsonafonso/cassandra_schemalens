"""Small SQL-like evidence extractor for the SmartRetail running example.

This is intentionally *not* a complete SQL parser.  It is a transparent demo
extractor for a controlled canonical workload.  The official SchemaLens path
should continue to use/extend the canonical SQL layer introduced in Step 2.1.

The extractor identifies only evidence needed by the running example:
- tables and aliases;
- equality predicates and BETWEEN ranges;
- ORDER BY / GROUP BY / LIMIT;
- aggregate functions;
- DISTINCT;
- TIME_BUCKET(...) and MODEL_SEGMENTS(...).

No family decision is made in this module.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


AGG_RE = re.compile(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", re.IGNORECASE)
FIELD_RE = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b")
FROM_JOIN_RE = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)\s+([A-Za-z_]\w*)", re.IGNORECASE)
EQ_PARAM_RE = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*=\s*:[A-Za-z_]\w*", re.IGNORECASE)
BETWEEN_RE = re.compile(
    r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s+BETWEEN\s+:[A-Za-z_]\w*\s+AND\s+:[A-Za-z_]\w*",
    re.IGNORECASE,
)
LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)


@dataclass
class SqlEvidence:
    operation: str
    entities: list[str]
    aliases: dict[str, str]
    equality_fields: list[str]
    between_fields: list[str]
    projected_fields: list[str]
    order_fields: list[dict[str, str]]
    group_fields: list[str]
    aggregate_function_count: int
    distinct: bool
    limit_value: int | None
    time_bucketed: bool
    model_segment_requested: bool


def _section(sql: str, start_keyword: str, end_keywords: list[str]) -> str:
    upper = sql.upper()
    start = upper.find(start_keyword.upper())
    if start < 0:
        return ""
    start += len(start_keyword)
    ends = [upper.find(keyword.upper(), start) for keyword in end_keywords]
    ends = [idx for idx in ends if idx >= 0]
    end = min(ends) if ends else len(sql)
    return sql[start:end]


def _qualified_fields(text: str, aliases: dict[str, str]) -> list[str]:
    result: list[str] = []
    for alias, attribute in FIELD_RE.findall(text):
        entity = aliases.get(alias, alias)
        field = f"{entity}.{attribute}"
        if field not in result:
            result.append(field)
    return result


def extract_sql_evidence(sql: str) -> SqlEvidence:
    normalized = " ".join(sql.replace("\n", " ").split())
    operation_match = re.match(r"\s*(SELECT|UPDATE|INSERT|DELETE)\b", normalized, re.IGNORECASE)
    operation = operation_match.group(1).upper() if operation_match else "UNKNOWN"

    aliases: dict[str, str] = {}
    entities: list[str] = []
    for entity, alias in FROM_JOIN_RE.findall(normalized):
        aliases[alias] = entity
        if entity not in entities:
            entities.append(entity)

    select_part = _section(normalized, "SELECT", [" FROM "])
    where_part = _section(normalized, " WHERE ", [" GROUP BY ", " ORDER BY ", " LIMIT ", ";"])
    group_part = _section(normalized, " GROUP BY ", [" ORDER BY ", " LIMIT ", ";"])
    order_part = _section(normalized, " ORDER BY ", [" LIMIT ", ";"])

    equality_fields: list[str] = []
    for alias, attribute in EQ_PARAM_RE.findall(where_part):
        field = f"{aliases.get(alias, alias)}.{attribute}"
        if field not in equality_fields:
            equality_fields.append(field)

    between_fields: list[str] = []
    for alias, attribute in BETWEEN_RE.findall(where_part):
        field = f"{aliases.get(alias, alias)}.{attribute}"
        if field not in between_fields:
            between_fields.append(field)

    projected_fields = _qualified_fields(select_part, aliases)
    group_fields = _qualified_fields(group_part, aliases)

    order_fields: list[dict[str, str]] = []
    # ORDER BY can refer to a projected alias (e.g. bucket).  We keep that as
    # an unqualified field instead of pretending that it belongs to an entity.
    for term in [piece.strip() for piece in order_part.split(",") if piece.strip()]:
        direction_match = re.search(r"\b(ASC|DESC)\b", term, re.IGNORECASE)
        direction = direction_match.group(1).upper() if direction_match else "ASC"
        field_match = FIELD_RE.search(term)
        if field_match:
            alias, attribute = field_match.groups()
            field = f"{aliases.get(alias, alias)}.{attribute}"
        else:
            token = re.sub(r"\b(ASC|DESC)\b", "", term, flags=re.IGNORECASE).strip()
            field = token
        order_fields.append({"field": field, "direction": direction})

    limit_match = LIMIT_RE.search(normalized)
    limit_value = int(limit_match.group(1)) if limit_match else None

    return SqlEvidence(
        operation=operation,
        entities=entities,
        aliases=aliases,
        equality_fields=equality_fields,
        between_fields=between_fields,
        projected_fields=projected_fields,
        order_fields=order_fields,
        group_fields=group_fields,
        aggregate_function_count=len(AGG_RE.findall(normalized)),
        distinct=bool(re.search(r"\bSELECT\s+DISTINCT\b", normalized, re.IGNORECASE)),
        limit_value=limit_value,
        time_bucketed=bool(re.search(r"\bTIME_BUCKET\s*\(", normalized, re.IGNORECASE)),
        model_segment_requested=bool(re.search(r"\bMODEL_SEGMENTS\s*\(", normalized, re.IGNORECASE)),
    )
