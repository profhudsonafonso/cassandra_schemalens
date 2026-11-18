"""Fase 1 (cont.): E4 (estatísticas observadas em rubis_full) e E7 (validação do catálogo)."""
from __future__ import annotations

import ast
import statistics
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import yaml

from .common import RUN, SOURCE_KEYSPACE, V9, now_iso, read_json, write_json
from .stages import set_stage, stage_dir

CATALOG = V9 / "schemalens_v9/catalog.yaml"
SINCE = datetime(2010, 1, 1)
SIZES = {"ID": 4, "Integer": 4, "Float": 8, "Date": 8, "FK": 4}


def _scan(s, table: str, cols: list[str], slices: int, on_rows, progress):
    from cassandra.query import SimpleStatement
    lo, hi = -(2 ** 63), 2 ** 63 - 1
    step = (hi - lo) // slices
    bounds = [(lo + i * step, (lo + (i + 1) * step) if i < slices - 1 else hi) for i in range(slices)]
    lock = threading.Lock()
    done = [0]

    def work(i):
        a, b = bounds[i]
        op = ">=" if i == 0 else ">"
        st = SimpleStatement(f"SELECT {', '.join(cols)} FROM {SOURCE_KEYSPACE}.{table} WHERE token(id) {op} %s AND token(id) <= %s", fetch_size=5000)
        rows = [tuple(r) for r in s.execute(st, (a, b))]
        with lock:
            on_rows(rows)
            done[0] += 1
            progress(done[0], slices)
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(work, range(slices)))


def _dist(counter: Counter, parents: int) -> dict:
    vals = sorted(counter.values())
    zeros = max(0, parents - len(vals))
    full = [0] * zeros + vals

    def pct(p):
        return full[min(len(full) - 1, int(round(p / 100 * (len(full) - 1))))] if full else 0
    mean = sum(vals) / parents if parents else 0
    return {"parents": parents, "parents_with_children": len(vals), "zero_fraction": round(zeros / parents, 6) if parents else None,
            "mean": round(mean, 4), "p50": pct(50), "p90": pct(90), "p99": pct(99), "max": full[-1] if full else 0,
            "max_over_mean": round(full[-1] / mean, 3) if mean else None}


def e4():
    from cassandra.cluster import Cluster
    from cassandra.query import tuple_factory
    schema = read_json(RUN / "stages/E03_conceptual_schema/CONCEPTUAL_SCHEMA.json")
    counts = read_json(RUN / "stages/E02_source_data/SOURCE_COUNTS.json")["base_counts"]
    c = Cluster(["127.0.0.1"])
    s = c.connect()
    s.row_factory = tuple_factory
    s.default_timeout = 300
    plan = {"users": ["region_id"], "items": ["seller_id", "category_id", "end_date"], "bids": ["user_id", "item_id"],
            "comments": ["from_user_id", "to_user_id", "item_id"], "buynow": ["buyer_id", "item_id"]}
    fan = {}
    nulls = Counter()
    region_of = {}
    items_rc = Counter()
    seller_of = []
    since_ok = [0, 0]
    total_tables = len(plan)
    try:
        for k, (tbl, cols) in enumerate(plan.items()):
            cnt = {col: Counter() for col in cols if col.endswith("_id")}
            slices = 1024 if counts[tbl] > 1_000_000 else 128

            def on_rows(rows, _cols=cols, _cnt=cnt, _tbl=tbl):
                for col_i, col in enumerate(_cols):
                    if col in _cnt:
                        vals = [r[col_i + 1] for r in rows]   # r[0] is the id column
                        nulls[f"{_tbl}.{col}"] += sum(v is None for v in vals)
                        _cnt[col].update(v for v in vals if v is not None)
                if _tbl == "users":
                    for r in rows:
                        region_of[r[0]] = r[1]
                if _tbl == "items":
                    for r in rows:
                        seller_of.append((r[1], r[2]))
                        since_ok[0] += 1
                        since_ok[1] += r[3] is not None and r[3] >= SINCE

            def prog(d, n, _k=k, _t=tbl):
                set_stage("E4", progress={"text": f"tabela {_k + 1} de {total_tables} ({_t}): faixa {d} de {n}", "done": _k * 10000 + int(10000 * d / n), "total": total_tables * 10000})
            _scan(s, tbl, ["id"] + cols, slices, on_rows, prog)
            for col, ctr in cnt.items():
                fan[f"{tbl}.{col}"] = ctr
    finally:
        c.shutdown()
    for seller, cat in seller_of:
        items_rc[(region_of.get(seller), cat)] += 1
    rels = {}
    for r in schema["relationships"]:
        key = f"{r['from']}.{r['fk_attribute']}"
        rels[f"{r['from']}.{r['name']} -> {r['to']} (inverso: {r['to']}.{r['inverse']})"] = {
            "fk": key, "null_fk_rows": nulls[key], "from_side_mandatory_observed": nulls[key] == 0,
            "fanout_per_target": _dist(fan[key], counts[r["to"]])}
    regions, cats = counts["regions"], counts["categories"]
    composite = {"items por (seller.region_id, category_id)": _dist(items_rc, regions * cats)}
    width = {}
    for e, v in schema["entities"].items():
        b = sum((a["size"] or SIZES.get(a["type"], 8)) if a["type"] == "String" else SIZES.get(a["type"], 8) for a in v["attributes"])
        b += 4 * sum(1 for r in schema["relationships"] if r["from"] == e)
        width[e] = b
    out = stage_dir("E4") / "STATS.json"
    rec = {"source": SOURCE_KEYSPACE, "measured_at": now_iso(), "entity_counts": counts, "relationships": rels,
           "composite_partitions": composite,
           "range_selectivity": {"items.end_date >= 2010-01-01 (since fixo da carga)": round(since_ok[1] / since_ok[0], 6) if since_ok[0] else None},
           "estimated_row_bytes_model": width, "row_bytes_basis": "tamanhos de String do modelo NoSE; Integer/ID/FK 4 B; Float/Date 8 B"}
    write_json(out, rec)
    checks = {"relationships_10": len(rels) == 10, "all_from_sides_mandatory": all(v["from_side_mandatory_observed"] for v in rels.values()),
              "composite_35_measured": composite["items por (seller.region_id, category_id)"]["parents"] == regions * cats}
    return "PASS" if checks["relationships_10"] and checks["composite_35_measured"] else "FAIL", [out], checks, {}


# ------------------------------------------------------------------------------------------- E7
def e7():
    cat = yaml.safe_load(CATALOG.read_text())
    fields = set(cat["evidence_fields"])
    params = set(cat["parameters"])
    problems = []
    for fam in [f"C{i}" for i in range(9)]:
        f = cat["families"].get(fam)
        if not f:
            problems.append(f"{fam} ausente")
            continue
        for k in ("source", "provides", "supersedes", "maintenance", "activation"):
            if k not in f:
                problems.append(f"{fam}: falta {k}")
        for side, expr in (f.get("activation") or {}).items():
            try:
                tree = ast.parse(expr, mode="eval")
            except SyntaxError as e:
                problems.append(f"{fam}.{side}: sintaxe {e}")
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                    if node.value.id == "q" and node.attr not in fields:
                        problems.append(f"{fam}.{side}: campo desconhecido q.{node.attr}")
                    if node.value.id == "p" and node.attr not in params:
                        problems.append(f"{fam}.{side}: parâmetro desconhecido p.{node.attr}")
                if isinstance(node, ast.Name) and node.id not in ("q", "p", "len", "True", "False", "None"):
                    problems.append(f"{fam}.{side}: nome não permitido {node.id}")
            consts = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, (int, float)) and not isinstance(n.value, bool)]
            if any(v not in (0, 1, 2) for v in consts):
                problems.append(f"{fam}.{side}: limiar numérico literal no predicado (use parameters): {consts}")
    c9 = cat["families"].get("C9", {})
    out = stage_dir("E7") / "CATALOG_VALIDATION.json"
    write_json(out, {"catalog": str(CATALOG), "families": sorted(cat["families"]), "parameters": cat["parameters"],
                     "problems": problems, "c9": c9.get("status")})
    checks = {"families_C0_C8_complete": not [p for p in problems if "falta" in p or "ausente" in p],
              "predicates_quantitative_and_valid": not [p for p in problems if "falta" not in p and "ausente" not in p],
              "c9_reserved": c9.get("status") == "UNDEFINED_RESERVED"}
    return ("PASS" if all(checks.values()) else "FAIL"), [out, CATALOG], checks, {}
