"""Navigation maps derived from the E3 conceptual schema (runs/.../E03_conceptual_schema/CONCEPTUAL_SCHEMA.json)."""
from __future__ import annotations

from .common import RUN, read_json

_S = read_json(RUN / "stages/E03_conceptual_schema/CONCEPTUAL_SCHEMA.json", None)
if _S is None:
    raise RuntimeError("E3 has not produced CONCEPTUAL_SCHEMA.json yet")

REL = {e: {} for e in _S["entities"]}      # entity -> {forward relationship name: target entity}
FK = {e: {} for e in _S["entities"]}       # entity -> {forward relationship name: FK attribute}
INVERSE = {e: {} for e in _S["entities"]}  # target -> {inverse name: (source entity, FK attribute)}
for r in _S["relationships"]:
    REL[r["from"]][r["name"]] = r["to"]
    FK[r["from"]][r["name"]] = r["fk_attribute"]
    INVERSE[r["to"]][r["inverse"]] = (r["from"], r["fk_attribute"])
ATTR_TYPES = {e: {a["name"]: a["type"] for a in v["attributes"]} for e, v in _S["entities"].items()}
for r in _S["relationships"]:
    ATTR_TYPES[r["from"]][r["fk_attribute"]] = "FK"
COLUMN_TO_ATTR = {e: {c: a for a, c in v["source_columns"].items()} for e, v in _S["entities"].items()}
SCHEMA = _S


def from_full_row(entity: str, row: dict) -> dict:
    m = COLUMN_TO_ATTR[entity]
    return {m[c]: v for c, v in row.items() if c in m}
