"""V9 independent oracle: evaluates the E6 contract over a source state read from rubis_full.

No QueryPlan or physical table of any deployment is used to compute expected answers.  The cohort keyspace
of V8 (rubis_v8_c008_auto_norm) is read only for IDENTITIES (which items/users/bids/comments/buynow form the
cohort); every attribute value comes from rubis_full.  Request histories come from the reused, seeded
workload generator; warm-up requests are evaluated too (their writes change the state).
"""
from __future__ import annotations

import copy
import re
from datetime import datetime
from typing import Any

from .contract import CONTRACT
from .schema_map import FK, REL, from_full_row

COHORT_KEYSPACE = "rubis_v8_c008_auto_norm"
COHORT_ID_TABLES = {"items": "items", "users": "users", "bids": "bids", "comments": "comments", "buynow": "buynow"}


# ------------------------------------------------------------------------------------------- state
def load_state(session) -> tuple[dict, dict]:
    ids = {}
    for ent, tbl in COHORT_ID_TABLES.items():
        ids[ent] = sorted(int(r["id"]) for r in session.execute(f"SELECT id FROM {COHORT_KEYSPACE}.{tbl}"))
    state = {e: {} for e in ("categories", "regions", "users", "items", "bids", "comments", "buynow")}
    for e in ("categories", "regions"):
        for r in session.execute(f"SELECT * FROM rubis_full.{e}"):
            state[e][r["id"]] = from_full_row(e, r)

    def fetch(entity: str, wanted: list[int]):
        from cassandra.concurrent import execute_concurrent_with_args
        stmt = session.prepare(f"SELECT * FROM rubis_full.{entity} WHERE id=?")
        todo = [i for i in wanted if i not in state[entity]]
        for ok, res in execute_concurrent_with_args(session, stmt, [(i,) for i in todo], concurrency=64):
            row = res.one() if ok else None
            if row:
                state[entity][row["id"]] = from_full_row(entity, row)

    for e in ("items", "bids", "comments", "buynow", "users"):
        fetch(e, ids[e])
    # closure of users referenced by the cohort (sellers, bidders, commenters, buyers): needed to navigate
    ref_users = set()
    for e, fks in (("items", ["seller_id"]), ("bids", ["user_id"]), ("comments", ["from_user_id", "to_user_id"]), ("buynow", ["buyer_id"])):
        for row in state[e].values():
            ref_users.update(int(row[k]) for k in fks if row.get(k) is not None)
    fetch("users", sorted(ref_users))
    prov = {"cohort_keyspace": COHORT_KEYSPACE, "cohort_counts": {k: len(v) for k, v in ids.items()},
            "state_counts": {k: len(v) for k, v in state.items()}, "closure_users_added": len(state["users"]) - len(ids["users"])}
    return state, prov


# ------------------------------------------------------------------------------------------- eval
def _src(source: str, params: dict, ctx: dict):
    for alt in source.split("|"):
        kind, _, name = alt.partition(":")
        if kind == "param" and name in params:
            return params[name]
        if kind == "const":
            return float(name) if "." in name else (int(name) if name.lstrip("-").isdigit() else name)
        if kind == "ctx":
            key, _, sub = name.partition(".")
            if key in ctx:
                v = ctx[key]
                if sub:
                    if isinstance(v, dict) and sub in v:
                        return v[sub]
                    continue
                return v
    return None


def _nav(state: dict, entity: str, row: dict, ref: str):
    """Resolve attr_ref from `row` of `entity`; returns (value, ok)."""
    parts = ref.split(".")
    cur_e, cur = entity, row
    for rel in parts[:-1]:
        to = REL[cur_e][rel]
        fk = cur.get(FK[cur_e][rel])
        cur = state[to].get(int(fk)) if fk is not None else None
        cur_e = to
        if cur is None:
            return None, False
    return cur.get(parts[-1]), True


def _index(state: dict, ent: str, attr: str) -> dict:
    idx = state.setdefault("_idx", {})
    key = (ent, attr)
    if key not in idx:
        d = {}
        for r in state[ent].values():
            d.setdefault(r.get(attr), []).append(r)
        idx[key] = d
    return idx[key]


def _cmp(a, op, b) -> bool:
    if a is None or b is None:
        return False
    if isinstance(a, datetime) and isinstance(b, str):
        b = datetime.fromisoformat(b)
    if op == "=":
        return a == b
    if op == ">=":
        return a >= b
    raise ValueError(op)


def eval_read(state: dict, sid: int, params: dict, ctx: dict) -> list[dict]:
    c = CONTRACT[sid]
    ent = c["entity"]
    conds = [(ref, op, _src(src, params, ctx)) for ref, op, src in c["where"]]
    # point lookup on the entity key: direct access (same result as a scan, much faster)
    pk = [v for ref, op, v in conds if ref == "id" and op == "="]
    plain = [(ref, v) for ref, op, v in conds if op == "=" and "." not in ref and ref != "id"]
    if pk:
        cand = [state[ent][int(pk[0])]] if pk[0] is not None and int(pk[0]) in state[ent] else []
    elif plain:
        ref, v = plain[0]
        cand = list(_index(state, ent, ref).get(v, ()))
    else:
        cand = list(state[ent].values())
    rows = []
    for r in cand:
        ok = True
        for ref, op, v in conds:
            val, found = _nav(state, ent, r, ref)
            if isinstance(v, (int, float)) and isinstance(val, (int, float)) and op == "=":
                ok = found and val == v
            else:
                ok = found and _cmp(val, op, v)
            if not ok:
                break
        if ok and all(_nav(state, ent, r, rel + ".id")[1] for rel in c["require"]):
            rows.append(r)
    for ref, direction in reversed(c["order"]):
        rows.sort(key=lambda r, ref=ref: _nav(state, ent, r, ref)[0], reverse=(direction == "desc"))
    if c["distinct"]:
        seen, out = set(), []
        for r in rows:
            k = _nav(state, ent, r, c["distinct"])[0]
            if k not in seen:
                seen.add(k)
                out.append(r)
        rows = out
    if c["limit"] is not None:
        rows = rows[: c["limit"]]
    result = []
    for r in rows:
        o = {}
        for key, ref in c["project"]:
            val, found = _nav(state, ent, r, ref)
            if not found and ref in c["fallback"]:
                val = c["fallback"][ref].format(**r)
            o[key] = val
        result.append(o)
    for k, (idx, key) in c["ctx_out"].items():
        if len(result) > idx:
            ctx[k] = result[idx] if key is None else result[idx][key]
    return result


_TOKEN = re.compile(r"(ctx:[\w.|:]+|param:\w+|const:[\w.\-]+|value:\w+|[-+(),]|max0|max|\d+)")


def _expr(expr: str, params: dict, ctx: dict):
    expr = expr.strip()
    m = re.fullmatch(r"(max0|max)\((.*)\)", expr)
    if m:
        if m.group(1) == "max0":
            return max(0, _expr(m.group(2), params, ctx))
        a, b = [x.strip() for x in m.group(2).split(",")]
        return max(float(_expr(a, params, ctx)), float(_expr(b, params, ctx)))
    for op in (" + ", " - "):
        if op in expr:
            a, b = expr.split(op, 1)
            x, y = _expr(a, params, ctx), _expr(b, params, ctx)
            return x + y if op == " + " else x - y
    if expr.isdigit():
        return int(expr)
    v = _src(expr, params, ctx)
    return v


def _typed(entity: str, attr: str, v):
    from .schema_map import ATTR_TYPES
    t = ATTR_TYPES[entity].get(attr)
    if v is None:
        return None
    if t == "Date":
        return datetime.fromisoformat(v) if isinstance(v, str) else v
    if t == "Float":
        return float(v)
    if t in ("Integer", "ID", "FK"):
        return int(v)
    return v


def eval_write(state: dict, sid: int, params: dict, ctx: dict) -> list[dict]:
    c = CONTRACT[sid]
    ent = c["entity"]
    vals = {}
    if c["op"] == "INSERT":
        for a, src in c["values"].items():
            vals[a] = _typed(ent, a, _src(src, params, ctx))
        state[ent][vals["id"]] = vals
        for (e, attr), d in state.get("_idx", {}).items():
            if e == ent:
                d.setdefault(vals.get(attr), []).append(vals)
    else:
        key = int(_src(c["key"], params, ctx))
        for a, ex in c["values"].items():
            vals[a] = _typed(ent, a, _expr(ex, params, ctx))
        if key in state[ent]:
            state[ent][key].update(vals)
            for attr in vals:
                state.get("_idx", {}).pop((ent, attr), None)   # indexed attribute changed: rebuild lazily
    event = {}
    for k, src in c["event"].items():
        event[k] = vals[src.split(":", 1)[1]] if src.startswith("value:") else _typed_event(_src(src, params, ctx))
    return [event]


def _typed_event(v):
    return int(v) if isinstance(v, str) and v.isdigit() else v


def evaluate(state: dict, sid: int, params: dict, ctx: dict) -> list[dict]:
    return eval_read(state, sid, params, ctx) if CONTRACT[sid]["kind"] == "read" else eval_write(state, sid, params, ctx)


def fresh(state: dict) -> dict:
    return copy.deepcopy(state)
