"""Parsers for the NoSE RUBiS model (models/rubis.rb) and workload (workloads/rubis.rb), commit d275634.

Pure text parsing; no Ruby needed.  The NoSE query language used by rubis.rb:
  SELECT <proj> FROM <path> WHERE <preds> [ORDER BY <attr>] [LIMIT n] -- <id>
  INSERT INTO <entity> SET a=?, ... [AND CONNECT TO rel(?), ...] -- <id>
  UPDATE <entity> SET a=?, ... WHERE <entity>.id=? -- <id>
"""
from __future__ import annotations

import re
from pathlib import Path

from .common import V9

MODEL_RB = V9 / "reused/inputs/historical_sources/models/rubis.rb"
WORKLOAD_RB = V9 / "reused/workloads/rubis.rb"
MIX_NAMES = {"browsing": "browsing", "bidding": "bidding", "write_medium": "update10x", "write_heavy": "update100x"}


def parse_model(path: Path = MODEL_RB) -> dict:
    text = path.read_text()
    entities = {}
    for m in re.finditer(r"\(Entity '(\w+)' do(.*?)end\) \* ([\d_]+)", text, re.S):
        name, body, count = m.group(1), m.group(2), int(m.group(3).replace("_", ""))
        attrs = []
        for line in body.splitlines():
            f = re.match(r"\s*(ID|String|Integer|Float|Date)\s+'(\w+)'(?:,\s*(\d+))?(?:,\s*count:\s*([\d_]+))?", line)
            if f:
                attrs.append({"name": f.group(2), "type": f.group(1), "size": int(f.group(3)) if f.group(3) else None,
                              "distinct_count_optimizer": int(f.group(4).replace("_", "")) if f.group(4) else None})
        entities[name] = {"attributes": attrs, "count_optimizer": count}
    rels = []
    for m in re.finditer(r"HasOne\s+'(\w+)',\s*'(\w+)',\s*\{'(\w+)'\s*=>\s*'(\w+)'\}", text):
        rels.append({"name": m.group(1), "inverse": m.group(2), "from": m.group(3), "to": m.group(4), "kind": "HasOne"})
    return {"entities": entities, "relationships": rels}


def _strings(block: str) -> str:
    return "".join(re.findall(r"'((?:[^'\\]|\\.)*)'", block))


def parse_workload(path: Path = WORKLOAD_RB) -> dict:
    text = path.read_text()
    groups = []
    for m in re.finditer(r"Group '(\w+)',(.*?) do\n(.*?)\n  end", text, re.S):
        name, wtxt, body = m.group(1), m.group(2), m.group(3)
        weights = {}
        for w in re.finditer(r"(\w+):\s*([\d.]+(?:\s*\*\s*[\d.]+)?)", wtxt):
            expr = w.group(2)
            val = 1.0
            for part in expr.split("*"):
                val *= float(part)
            weights[MIX_NAMES[w.group(1)]] = round(val, 6)
        stmts = []
        for q in re.finditer(r"Q ('.*?')\s*$(?=\n\s*(?:Q |#|$))", body + "\n", re.S | re.M):
            pass
        # statements: join consecutive quoted strings that belong to one Q
        for chunk in re.split(r"\n\s*Q ", "\n" + body)[1:]:
            chunk = chunk.split("\n    #")[0]
            sql = _strings(chunk)
            sm = re.search(r"--\s*(\d+)\s*$", sql)
            stmts.append({"statement_id": int(sm.group(1)), "text": sql[: sm.start()].strip()})
        groups.append({"name": name, "weights": {k: weights.get(k, 0.0) for k in MIX_NAMES.values()}, "statements": stmts})
    return {"groups": groups}


def parse_statement(text: str) -> dict:
    t = " ".join(text.split())
    if t.startswith("SELECT"):
        m = re.match(r"SELECT (.*?) FROM ([\w.]+) WHERE (.*?)(?: ORDER BY ([\w.]+))?(?: LIMIT (\d+))?$", t)
        proj = [p.strip() for p in m.group(1).split(",")]
        preds = []
        for p in re.split(r"\s+AND\s+", m.group(3)):
            pm = re.match(r"([\w.]+)\s*(>=|<=|=|>|<)\s*(\?|\d+)", p.strip())
            preds.append({"attr": pm.group(1), "op": pm.group(2), "value": pm.group(3)})
        return {"kind": "read", "projection": proj, "path": m.group(2).split("."), "predicates": preds,
                "order_by": m.group(4), "limit": int(m.group(5)) if m.group(5) else None}
    if t.startswith("INSERT"):
        m = re.match(r"INSERT INTO (\w+) SET (.*?)(?: AND CONNECT TO (.*))?$", t)
        sets = [s.split("=")[0].strip() for s in m.group(2).split(",")]
        consts = {s.split("=")[0].strip(): s.split("=")[1].strip() for s in m.group(2).split(",") if s.split("=")[1].strip() != "?"}
        conns = re.findall(r"(\w+)\(\?\)", m.group(3) or "")
        return {"kind": "write", "op": "INSERT", "entity": m.group(1), "set": sets, "constants": consts, "connect": conns}
    if t.startswith("UPDATE"):
        m = re.match(r"UPDATE (\w+) SET (.*?) WHERE ([\w.]+)\s*=\s*\?$", t)
        return {"kind": "write", "op": "UPDATE", "entity": m.group(1), "set": [s.split("=")[0].strip() for s in m.group(2).split(",")],
                "key": m.group(3)}
    raise ValueError(t)
