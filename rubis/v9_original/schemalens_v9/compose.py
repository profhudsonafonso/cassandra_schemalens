"""E15 — automatic composition of H by integer linear programming (PuLP/CBC); E16 — frozen Hbench.

Decision variables: x_t (table t of C is in the schema), y_{q,o} (read q uses QueryPlan option o),
z_{w,E} (write w issues one SupportQuery on entity E).  Constraints: one route per read; a route needs its
tables; a maintained table needs a lookup table for its support attributes; tables that a write cannot
maintain are excluded; every inserted entity keeps one complete copy (D-V9-010); `supersedes` forbids a
family table together with the tables it replaces.  Objective for a mix:
  Σ_q f_q·(a·reads + c·rows) + Σ_w f_w·(Σ_t b·writes(w,t)·x_t + Σ_E (a + c)·z_{w,E})
with a, b, c from E14.  Everything is deterministic (CBC, fixed ordering, no randomness).
"""
from __future__ import annotations

import itertools
import json
from collections import defaultdict

import pulp

from . import phase2 as F
from . import routes as R
from .common import RUN, now_iso, read_json, sha256_obj, write_json
from .contract import CONTRACT, READ_IDS, WRITE_IDS
from .phase2_stages import generate
from .stages import stage_dir

WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
OPTIONAL = {"C2", "C3", "C4", "C5", "C6", "C7", "C8"}
EPS_STORAGE = 1e-4   # D-V9-011: tie-break only (cost per GB stored), keeps the smallest schema among equal-cost ones
V8_NORM_KEYSPACE = "rubis_v8_c008_auto_norm"


def coefficients():
    return read_json(RUN / "stages/E14_cost_model/COST_MODEL.json")["coefficients"]


def freqs():
    return read_json(RUN / "stages/E05_workload/WORKLOAD.json")["statement_frequency_per_request"]


def table_rows(t: dict, counts: dict) -> int:
    return counts[t["row_entity"]]


class Model:
    def __init__(self, C: list[dict], knobs: dict | None = None, allowed=None):
        self.knobs = {**F.DEFAULT_KNOBS, **(knobs or {})}
        self.C = [t for t in C if allowed is None or allowed(t)]
        self.by = {t["canonical_id"]: t for t in self.C}
        self.opts = {q: R.read_options(q, self.C) for q in READ_IDS}
        self.wn = {w: {t["canonical_id"]: R.write_needs(w, t) for t in self.C} for w in WRITE_IDS}
        st, self.counts = F.stats()
        self.coef = coefficients()
        self.f = freqs()

    def feasible(self) -> bool:
        return all(self.opts[q] for q in READ_IDS)

    def solve(self, weights: dict, extra=None, no_good: list[set] = ()) -> dict | None:
        a, b, c = self.coef["a_read_ms"], self.coef["b_write_ms"], self.coef["c_row_ms"]
        prob = pulp.LpProblem("H", pulp.LpMinimize)
        x = {t: pulp.LpVariable(f"x_{t}", cat="Binary") for t in sorted(self.by)}
        y = {(q, i): pulp.LpVariable(f"y_{q}_{i}", cat="Binary") for q in READ_IDS for i in range(len(self.opts[q]))}
        z = {}
        read_cost, write_cost = [], []
        for q in READ_IDS:
            prob += pulp.lpSum(y[(q, i)] for i in range(len(self.opts[q]))) == 1
            for i, o in enumerate(self.opts[q]):
                for t in [o["main"]] + [l[3] for l in o["lookups"]]:
                    prob += y[(q, i)] <= x[t]
                read_cost.append(weights["read_freq"][q] * (a * o["reads"] + c * o["rows"]) * y[(q, i)])
        for w in WRITE_IDS:
            for tid, n in self.wn[w].items():
                if not n["affected"]:
                    continue
                if not n["maintainable"]:
                    prob += x[tid] == 0
                    continue
                write_cost.append(weights["write_freq"][w] * b * n["writes"] * x[tid])
                for e2, attrs in n["support"].items():
                    L = [l["canonical_id"] for l in R._lookup_tables(self.C, e2, set(attrs))]
                    if not L:
                        prob += x[tid] == 0
                        continue
                    prob += x[tid] <= pulp.lpSum(x[l] for l in L)
                    if (w, e2) not in z:
                        z[(w, e2)] = pulp.LpVariable(f"z_{w}_{e2}", cat="Binary")
                        write_cost.append(weights["write_freq"][w] * (a + c) * z[(w, e2)])
                    prob += z[(w, e2)] >= x[tid]
            cw = CONTRACT[w]
            if cw["op"] == "INSERT":   # D-V9-010: one complete copy of every inserted entity
                full = [t["canonical_id"] for t in self.C if t["row_entity"] == cw["entity"] and set(cw["values"]) <= R._avail(t) | {"id"}]
                prob += pulp.lpSum(x[t] for t in full) >= 1
        if self.knobs["supersedes"]:
            for t in self.C:
                for sup in t["supersedes"]:
                    for u in self.C:
                        if u["canonical_id"] == t["canonical_id"]:
                            continue
                        if any(o["family"] == sup["family"] and o["statement"] == sup["statement"] for o in u["origin_all"]):
                            prob += x[t["canonical_id"]] + x[u["canonical_id"]] <= 1
        storage = pulp.lpSum(F.spec_bytes(self.by[t]) * table_rows(self.by[t], self.counts) / 1e9 * x[t] for t in x)
        prob += (weights.get("w_read", 1) * pulp.lpSum(read_cost) + weights.get("w_write", 1) * pulp.lpSum(write_cost)
                 + (weights.get("w_storage", 0) + EPS_STORAGE) * storage)
        for ng in no_good:   # exclude previously found table sets
            prob += pulp.lpSum(x[t] for t in ng) - pulp.lpSum(x[t] for t in x if t not in ng) <= len(ng) - 1
        if extra:
            extra(prob, x)
        status = prob.solve(pulp.PULP_CBC_CMD(msg=False, threads=1))
        if pulp.LpStatus[status] != "Optimal":
            return None
        chosen = sorted(t for t in x if x[t].value() > 0.5)
        return {"tables": chosen, "objective": pulp.value(prob.objective),
                "read_cost": pulp.value(pulp.lpSum(read_cost)), "write_cost": pulp.value(pulp.lpSum(write_cost)),
                "storage_gb": pulp.value(storage)}


def mix_weights(mix: str) -> dict:
    f = freqs()[mix]
    return {"read_freq": {q: f[str(q)] for q in READ_IDS}, "write_freq": {w: f[str(w)] for w in WRITE_IDS}}


def predicted(S: list[dict]) -> dict:
    """Exact predicted cost per mix of a concrete schema (routes chosen by the same cost function)."""
    co = coefficients()
    a, b, c = co["a_read_ms"], co["b_write_ms"], co["c_row_ms"]
    ev = R.evaluate(S, cost=lambda o: a * o["reads"] + c * o["rows"])
    out = {}
    for mix in WORKLOADS:
        f = freqs()[mix]
        rc = sum(f[str(q)] * (a * ev["plans"][q]["reads"] + c * ev["plans"][q]["rows"]) for q in READ_IDS if q in ev["plans"])
        wc = sum(f[str(w)] * (b * ev["plans"][w]["writes"] + (a + c) * ev["plans"][w]["reads"]) for w in WRITE_IDS if w in ev["plans"])
        out[mix] = {"read_cost": round(rc, 6), "write_cost": round(wc, 6), "total": round(rc + wc, 6)}
    return {"per_mix": out, "valid": ev["valid"], "invalid": ev["invalid"], "plans": ev["plans"]}


def lineage(S: list[dict]) -> list[dict]:
    out = []
    for t in S:
        out.append({"table": t["name"], "canonical_id": t["canonical_id"], "families": t["families"], "candidates": t["members"],
                    "origin": [{"statement": o["statement"], "family": o["family"], "evidence": o["evidence"]} for o in t["origin_all"]],
                    "supersedes": t["supersedes"], "cover": t.get("cover", [])})
    return out


def v8_norm_structure() -> dict:
    from cassandra.cluster import Cluster
    cl = Cluster(["127.0.0.1"])
    s = cl.connect()
    try:
        out = defaultdict(lambda: {"partition": [], "clustering": [], "columns": []})
        for r in s.execute("SELECT table_name, column_name, kind, position FROM system_schema.columns WHERE keyspace_name=%s", (V8_NORM_KEYSPACE,)):
            k = {"partition_key": "partition", "clustering": "clustering", "regular": "columns"}[r.kind]
            out[r.table_name][k].append((r.position, r.column_name))
        return {t: {k: [c for _, c in sorted(v[k])] for k in v} for t, v in out.items()}
    finally:
        cl.shutdown()


def compose(knobs: dict | None = None) -> dict:
    """knobs=None: the full method.  Ablations (Fase 7) pass one neutralised criterion (D-V9-013)."""
    knobs = {**F.DEFAULT_KNOBS, **(knobs or {})}
    ev, act, P, C, fus = generate(knobs)
    cov = read_json(RUN / "stages/E13_route_synthesis/CANONICAL_WITH_COVER.json")["tables"] if knobs == F.DEFAULT_KNOBS else []
    cover = {t["canonical_id"]: t.get("cover", []) for t in cov}
    for t in C:
        t["cover"] = cover.get(t["canonical_id"], [])
    m = Model(C, knobs=knobs)
    if not knobs["cost_selection"]:   # no calibrated cost model: every operation weighs 1, rows weigh 0 (D-V9-013)
        m.coef = {"a_read_ms": 1.0, "b_write_ms": 1.0, "c_row_ms": 0.0, "d_request_ms": 0.0}
    found, H = [], []

    def add(sol, how, label):
        key = tuple(sol["tables"])
        for h in H:
            if tuple(h["tables"]) == key:
                h["selected_by"].append(how)
                return h
        h = {"tables": sol["tables"], "selected_by": [how], "label": label}
        H.append(h)
        return h
    # 1) one optimum per workload
    for mix in WORKLOADS:
        sol = m.solve(mix_weights(mix))
        add(sol, f"ótimo da carga {mix}", f"opt_{mix}")
    # 2) Pareto front (read cost, write cost, storage) under the bidding mix, by weighted sums on a fixed grid
    grid = [(1, 1, 0), (1, 0.25, 0), (1, 4, 0), (1, 1, 0.5), (1, 1, 2), (0.25, 1, 0), (4, 1, 0), (1, 0, 0), (0, 1, 0.01), (1, 1, 10)]
    pareto_pts = []
    for wr, ww, ws in grid:
        wts = {**mix_weights("bidding"), "w_read": wr, "w_write": ww, "w_storage": ws}
        sol = m.solve(wts)
        if sol:
            pareto_pts.append(sol)
    nd = [p for p in pareto_pts if not any((q["read_cost"] <= p["read_cost"] + 1e-9 and q["write_cost"] <= p["write_cost"] + 1e-9 and q["storage_gb"] <= p["storage_gb"] + 1e-9)
                                           and (q["read_cost"] < p["read_cost"] - 1e-9 or q["write_cost"] < p["write_cost"] - 1e-9 or q["storage_gb"] < p["storage_gb"] - 1e-9)
                                           for q in pareto_pts)]
    for i, p in enumerate(sorted(nd, key=lambda p: (p["read_cost"], p["write_cost"]))):
        add(p, "fronteira de Pareto (leitura × escrita × espaço, mix bidding)", f"pareto_{i + 1}")
    # 3) k-best distinct (bidding mix) until |H| >= 5
    k = 0
    while len(H) < 5 and k < 20:
        sol = m.solve(mix_weights("bidding"), no_good=[set(h["tables"]) for h in H])
        if not sol:
            break
        add(sol, "k-melhores distintos (mix bidding)", f"kbest_{k + 1}")
        k += 1
    # anchors (marked; not optimiser output)
    anchors = []
    m0 = Model(C, knobs={"supersedes": False}, allowed=lambda t: bool(set(t["families"]) & {"C0", "C1"}))
    a1 = m0.solve(mix_weights("bidding")) if m0.feasible() else None
    if a1:
        anchors.append({"tables": a1["tables"], "label": "anchor_sl_norm", "selected_by": ["âncora: gerador com famílias opcionais desligadas (só C0 e C1)"]})
    c5 = [t for t in C if "C5" in t["families"] and t["row_entity"] == "bids"]
    if a1 and c5:
        t5 = c5[0]["canonical_id"]
        superseded = {u["canonical_id"] for u in C for sup in c5[0]["supersedes"] for o in u["origin_all"]
                      if o["family"] == "C1" and o["statement"] == sup["statement"] and u["canonical_id"] != t5}
        tabs = sorted((set(a1["tables"]) - superseded) | {t5})
        anchors.append({"tables": tabs, "label": "anchor_c5_substitution", "selected_by": ["âncora: C5 como substituição (tabela fundida de lances por item no lugar das tabelas individuais)"],
                        "superseded_removed": sorted(superseded & set(a1["tables"]))})
    for an in anchors:
        same = [h["label"] for h in H if h["tables"] == an["tables"]]
        an["coincides_with"] = same
    by = {t["canonical_id"]: t for t in C}
    schemas = []
    for i, h in enumerate(H + anchors):
        S = [by[t] for t in h["tables"]]
        pr = predicted(S)
        fams = sorted({f for t in S for f in t["families"]})
        schemas.append({"schema_id": f"H{i + 1:02d}" if h not in anchors else h["label"].upper(), "label": h["label"], "anchor": h in anchors,
                        "selected_by": h["selected_by"], "tables": [by[t]["name"] for t in h["tables"]], "canonical_ids": h["tables"], "physical_ids": sorted(by[t]["physical_id"] for t in h["tables"]),
                        "table_count": len(S), "families": fams, "ddl": [by[t]["ddl"] for t in h["tables"]],
                        "predicted_cost": pr["per_mix"], "valid": pr["valid"], "invalid": pr["invalid"], "routes": pr["plans"],
                        "lineage": lineage(S), "superseded_removed": h.get("superseded_removed"), "coincides_with": h.get("coincides_with")})
    # comparison of the SL-norm anchor with V8 AUTO_H_norm (19 tables)
    v8 = v8_norm_structure()
    comp = None
    if anchors:
        a = next(s for s in schemas if s["label"] == "anchor_sl_norm")
        mine = [by[t] for t in a["canonical_ids"]]
        comp = {"v8_tables": sorted(v8), "v8_count": len(v8), "v9_tables": [t["name"] for t in mine], "v9_count": len(mine)}
    return {"generated_at": now_iso(), "H": [s for s in schemas if not s["anchor"]], "anchors": [s for s in schemas if s["anchor"]],
            "v8_auto_h_norm_structure": v8, "anchor_vs_v8": comp, "pareto_candidates": pareto_pts, "grid": grid}
