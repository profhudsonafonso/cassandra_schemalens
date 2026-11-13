"""Fase 2: E8 (fragmentos e raízes), E9 (matriz de evidências), E10 (ativação), E11 (especificações P),
E12 (canonicalização e fusão segura).  E13 (rotas) está em routes.py.

Everything is computed from: contract (E6), conceptual schema (E3), statistics (E4), workload (E5) and the
catalogue (E7).  A `knobs` dict lets the criteria ablations of Fase 7 neutralise one criterion at a time
(documented in DECISION_LOG); the full method uses DEFAULT_KNOBS.
"""
from __future__ import annotations

import ast
import json
from collections import defaultdict
from types import SimpleNamespace

import yaml

from .common import RUN, sha256_obj, write_json
from .contract import CONTRACT, READ_IDS, WRITE_IDS
from .phase1b import CATALOG, SIZES
from .schema_map import ATTR_TYPES, FK, REL, SCHEMA
from .stages import stage_dir

WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
FAMILIES = [f"C{i}" for i in range(9)]
DEFAULT_KNOBS = {"sharedness": True, "write_exposure": True, "access_profile": True, "support_sharing": True,
                 "relationships": True, "supersedes": True, "cost_selection": True, "canonicalization": True}


def stats():
    from .common import read_json
    return read_json(RUN / "stages/E04_observed_stats/STATS.json"), read_json(RUN / "stages/E02_source_data/SOURCE_COUNTS.json")["base_counts"]


def workload():
    from .common import read_json
    return read_json(RUN / "stages/E05_workload/WORKLOAD.json")


def catalog():
    return yaml.safe_load(CATALOG.read_text())


# ------------------------------------------------------------------------------------------- refs
def resolve(entity: str, ref: str) -> tuple[str, str, list[tuple[str, str, str]]]:
    """-> (owning entity, attribute, hops[(from, rel, to)])"""
    parts = ref.split(".")
    e, hops = entity, []
    for rel in parts[:-1]:
        to = REL[e][rel]
        hops.append((e, rel, to))
        e = to
    return e, parts[-1], hops


def gattr(entity: str, ref: str) -> str:
    e, a, _ = resolve(entity, ref)
    return f"{e}.{a}"


def attr_bytes(entity: str, ref: str) -> int:
    e, a, _ = resolve(entity, ref)
    t = ATTR_TYPES[e][a]
    if t == "String":
        size = next(x["size"] for x in SCHEMA["entities"][e]["attributes"] if x["name"] == a)
        return size or 8
    return SIZES.get(t, 8)


def fk_target(entity: str, attr: str) -> str | None:
    for rel, fk in FK[entity].items():
        if fk == attr:
            return REL[entity][rel]
    return None


def refs_of(c: dict) -> dict:
    return {"where": [r for r, _, _ in c["where"]], "project": [r for _, r in c["project"]], "order": [r for r, _ in c["order"]],
            "distinct": [c["distinct"]] if c["distinct"] else [], "require": [f"{x}.id" for x in c["require"]]}


def needed_refs(c: dict) -> list[str]:
    seen, out = set(), []
    for group in refs_of(c).values():
        for r in group:
            if r not in seen:
                seen.add(r)
                out.append(r)
    return out


# ------------------------------------------------------------------------------------------- E8
def fragments() -> dict:
    out = {}
    for sid, c in CONTRACT.items():
        if c["kind"] == "read":
            ent = c["entity"]
            F, edges = {ent}, set()
            for r in needed_refs(c):
                for a, rel, b in resolve(ent, r)[2]:
                    F |= {a, b}
                    edges.add((a, rel, b))
            for r, op, _ in c["where"]:
                e, a, _ = resolve(ent, r)
                t = fk_target(e, a)
                if t:
                    F.add(t)
                    edges.add((e, next(k for k, v in FK[e].items() if v == a), t))
            result_entity = resolve(ent, c["distinct"])[0] if c["distinct"] else ent
            anchors = []
            for r, op, _ in c["where"]:
                if op != "=":
                    continue
                e, a, hops = resolve(ent, r)
                anchors.append(fk_target(e, a) or e)
        else:
            ent = c["entity"]
            F, edges = {ent}, set()
            for a in c["values"]:
                t = fk_target(ent, a)
                if t:
                    F.add(t)
                    edges.add((ent, next(k for k, v in FK[ent].items() if v == a), t))
            result_entity, anchors = ent, [ent]
        # centrality in the fragment graph (undirected)
        adj = defaultdict(set)
        for a, _, b in edges:
            adj[a].add(b)
            adj[b].add(a)

        def dists(src):
            d, frontier = {src: 0}, [src]
            while frontier:
                nxt = []
                for x in frontier:
                    for y in adj[x]:
                        if y not in d:
                            d[y] = d[x] + 1
                            nxt.append(y)
                frontier = nxt
            return d
        cen = {}
        for e in sorted(F):
            d = dists(e)
            others = [v for k, v in d.items() if k != e]
            cen[e] = {"avg_distance": round(sum(others) / len(others), 3) if others else 0.0, "max_distance": max(others) if others else 0,
                      "direct_links": len(adj[e])}
        most_central = min(sorted(F), key=lambda e: (cen[e]["avg_distance"], cen[e]["max_distance"], -cen[e]["direct_links"]))
        out[sid] = {"statement": sid, "kind": c["kind"], "fragment": sorted(F), "edges": sorted(f"{a}.{r}->{b}" for a, r, b in edges),
                    "root": result_entity, "root_rule": "papel semântico: entidade cujas instâncias formam as linhas do resultado (ou a entidade escrita)",
                    "anchors": anchors, "centrality": cen, "most_central": most_central,
                    "root_equals_most_central": result_entity == most_central,
                    "partition_orientation": "chave de partição = atributos de igualdade (âncora); direção de acesso âncora → raiz"}
    return out


# ------------------------------------------------------------------------------------------- E9
def _expected_rows(c: dict, eq_refs: list[str], st: dict, counts: dict) -> float:
    ent = c["entity"]
    if not eq_refs:
        return float(counts[ent])
    if eq_refs == ["id"]:
        return 1.0
    if len(eq_refs) == 1:
        e, a, hops = resolve(ent, eq_refs[0])
        if a == "dummy":
            return float(counts[e])
        t = fk_target(e, a)
        if t and not hops:
            for k, v in st["relationships"].items():
                if v["fk"] == f"{e}.{a}":
                    return float(v["fanout_per_target"]["mean"])
    gl = sorted(gattr(ent, r) for r in eq_refs)
    if gl == ["items.category_id", "users.region_id"]:
        return float(st["composite_partitions"]["items por (seller.region_id, category_id)"]["mean"])
    # independence approximation: N / Π(distinct values) (declared in the record)
    n = float(counts[ent])
    for r in eq_refs:
        e, a, _ = resolve(ent, r)
        t = fk_target(e, a)
        n /= counts[t] if t else 1
    return n


def evidence(knobs: dict | None = None) -> dict:
    knobs = {**DEFAULT_KNOBS, **(knobs or {})}
    st, counts = stats()
    wl = workload()
    freq = wl["statement_frequency_per_request"]
    rng = st["range_selectivity"]["items.end_date >= 2010-01-01 (since fixo da carga)"]
    frag = fragments()
    upd = {s: [f"{CONTRACT[s]['entity']}.{a}" for a in CONTRACT[s]["values"]] for s in WRITE_IDS if CONTRACT[s]["op"] == "UPDATE"}
    # base signatures for sharedness
    sig = {}
    for s in READ_IDS:
        c = CONTRACT[s]
        eq = tuple(sorted(gattr(c["entity"], r) for r, op, _ in c["where"] if op == "="))
        rg = tuple(sorted(gattr(c["entity"], r) for r, op, _ in c["where"] if op != "="))
        sig[s] = (c["entity"], eq, rg)
    rows_cache = {}
    for s in READ_IDS:
        c = CONTRACT[s]
        eqr = [r for r, op, _ in c["where"] if op == "="]
        rows_cache[s] = _expected_rows(c, eqr, st, counts) * (rng if any(op != "=" for _, op, _ in c["where"]) else 1.0)

    def push_order(s):   # order that must be pushed to the server (LIMIT over a partition larger than λ2)
        c = CONTRACT[s]
        if c["limit"] is not None and rows_cache[s] > catalog()["parameters"]["lambda2_rows"]:
            return tuple((gattr(c["entity"], r), d) for r, d in c["order"])
        return None
    groups = defaultdict(list)
    for s in READ_IDS:
        groups[sig[s]].append(s)
    share = {}
    for s in READ_IDS:
        mates = [m for m in groups[sig[s]] if push_order(m) is None or push_order(s) is None or push_order(m) == push_order(s)]
        share[s] = len(mates) if knobs["sharedness"] else 1
    out = {}
    for mix in WORKLOADS:
        active_writes = [w for w in WRITE_IDS if freq[mix][str(w)] > 0]
        upd_attrs = {a: [w for w in active_writes if a in upd.get(w, [])] for w in [0] for a in {x for w in active_writes for x in upd.get(w, [])}}
        rows = {}
        for s in READ_IDS:
            c = CONTRACT[s]
            ent = c["entity"]
            eqr = [r for r, op, _ in c["where"] if op == "="]
            rgr = [r for r, op, _ in c["where"] if op != "="]
            proj = [r for _, r in c["project"]]
            refs = needed_refs(c)
            nav = [r for r in refs if "." in r]
            proj_g = [gattr(ent, r) for r in proj]
            vol = sorted({g for g in proj_g if g in upd_attrs}) if knobs["write_exposure"] else []
            vol_copies = 0
            if vol:
                sigs = {sig[o] for o in READ_IDS if any(gattr(CONTRACT[o]["entity"], r) in vol for _, r in CONTRACT[o]["project"])}
                vol_copies = len(sigs)
            wr = sum(freq[mix][str(w)] for w in active_writes if any(a in upd.get(w, []) for a in vol))
            ratio = wr / freq[mix][str(s)] if freq[mix][str(s)] > 0 else 0.0
            eq_on_key = eqr == ["id"] and not nav
            # reverse hops from the equality anchor to the root not resolved by a FK of the root itself
            root = frag[s]["root"]
            rev = 0
            for r in eqr:
                e, a, hops = resolve(ent, r)
                if fk_target(e, a) and not (e == root and not hops):
                    rev += 1 + len(hops)
            if not knobs["relationships"]:
                rev = 0
            exp_rows = rows_cache[s]
            bound = min(exp_rows, c["limit"]) if c["limit"] is not None else exp_rows
            relaxed = None
            if len(eqr) >= 2:
                cands = []
                for drop in eqr:
                    keep = [r for r in eqr if r != drop]
                    cands.append(_expected_rows(c, keep, st, counts) / exp_rows if exp_rows else float("inf"))
                relaxed = round(min(cands), 4)
            missing = []
            proj_entities = len({resolve(ent, r)[0] for r in proj})
            nonroot = [r for r in proj if resolve(ent, r)[0] != root]
            fields = {"kind": "read", "root": root, "eq_attrs": eqr, "eq_on_key": eq_on_key, "range_attrs": rgr,
                      "order_attrs": [r for r, _ in c["order"]], "proj_attrs": proj, "proj_entities": proj_entities,
                      "path_len": len({(a, rel) for r in refs for a, rel, _ in resolve(ent, r)[2]}), "reverse_hops": rev, "limit": c["limit"],
                      "expected_rows": round(exp_rows, 4), "rows_read_bound": round(bound, 4), "sharedness": share[s],
                      "volatile_proj": vol, "volatile_copies": vol_copies, "volatile_update_ratio": round(ratio, 6),
                      "proj_bytes": sum(attr_bytes(ent, r) for r in proj),
                      "nonroot_proj_bytes": sum(attr_bytes(ent, r) for r in nonroot) if knobs["access_profile"] else 0,
                      "nonroot_proj_volatile": [gattr(ent, r) for r in nonroot if gattr(ent, r) in upd_attrs],
                      "support_attrs": nav if knobs["support_sharing"] else [],
                      "relaxed_ratio": relaxed if relaxed is not None else float("inf"), "missing_maintenance_keys": missing,
                      "mix_write_weight": round(sum(freq[mix][str(w)] for w in active_writes), 6),
                      "residual": {"client_filter": bool(rgr), "client_join": bool(nav), "client_order": bool(c["order"]),
                                   "client_limit": c["limit"] is not None, "client_distinct": bool(c["distinct"])},
                      "signature": list(map(str, sig[s]))}
            if not knobs["access_profile"]:
                fields.update({"expected_rows": 1.0, "rows_read_bound": 1.0, "relaxed_ratio": float("inf")})
            rows[s] = fields
        for w in WRITE_IDS:
            c = CONTRACT[w]
            rows[w] = {"kind": "write", "root": c["entity"], "eq_attrs": ["id"], "eq_on_key": True, "range_attrs": [], "order_attrs": [],
                       "proj_attrs": list(c["values"]), "proj_entities": 1, "path_len": 0, "reverse_hops": 0, "limit": None,
                       "expected_rows": 1.0, "rows_read_bound": 1.0, "sharedness": 1, "volatile_proj": [], "volatile_copies": 0,
                       "volatile_update_ratio": 0.0, "proj_bytes": 0, "nonroot_proj_bytes": 0, "nonroot_proj_volatile": [],
                       "support_attrs": [], "relaxed_ratio": float("inf"),
                       "missing_maintenance_keys": _missing_keys(w, rows, knobs), "mix_write_weight": 0.0,
                       "frequency": freq[mix][str(w)], "op": c["op"], "updated_attrs": upd.get(w, [])}
        out[mix] = rows
    return {"knobs": knobs, "fragments": frag, "matrix": out}


def _c1_template(s: int) -> dict:
    """Keys and columns that a fully materialised (C1) table for read s would hold, as refs on its entity."""
    c = CONTRACT[s]
    keys = [r for r, op, _ in c["where"] if op == "="] + [r for r, op, _ in c["where"] if op != "="] + [r for r, _ in c["order"]]
    keys += [c["distinct"]] if c["distinct"] else ["id"]
    seen, k2 = set(), []
    for r in keys:
        if r not in seen:
            seen.add(r)
            k2.append(r)
    return {"entity": c["entity"], "keys": k2, "columns": [r for r in needed_refs(c) if r not in seen]}


def _missing_keys(w: int, rows: dict, knobs: dict) -> list[str]:
    """Values a write needs for C1-style copies of its entity that are not among its parameters."""
    c = CONTRACT[w]
    ent = c["entity"]
    have = set(c["values"]) if c["op"] == "INSERT" else {"id"}
    missing = set()
    for s in READ_IDS:
        r = rows.get(s)
        if r is None or r["eq_on_key"]:
            continue
        t = _c1_template(s)
        if t["entity"] != ent:
            continue
        if c["op"] == "INSERT":
            for ref in t["keys"] + t["columns"]:
                if "." in ref:
                    missing.add(gattr(ent, ref))
        else:
            if set(c["values"]) & {resolve(ent, r)[1] for r in t["columns"] if "." not in r}:
                for ref in t["keys"]:
                    if "." not in ref and ref not in have:
                        missing.add(f"{ent}.{ref}")
    return sorted(missing) if knobs["support_sharing"] else []


# ------------------------------------------------------------------------------------------- E10
def activation(ev: dict) -> dict:
    cat = catalog()
    p = SimpleNamespace(**cat["parameters"])
    res = {}
    for mix, rows in ev["matrix"].items():
        res[mix] = {}
        for s, f in rows.items():
            q = SimpleNamespace(**f)
            act, terms = [], {}
            for fam in FAMILIES:
                expr = (cat["families"][fam].get("activation") or {}).get(f["kind"])
                if not expr:
                    continue
                if eval(compile(ast.parse(expr, mode="eval"), fam, "eval"), {"__builtins__": {}}, {"q": q, "p": p, "len": len}):
                    act.append(fam)
                    names = sorted({n.attr for n in ast.walk(ast.parse(expr, mode="eval")) if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "q"})
                    terms[fam] = {n: f[n] for n in names}
            res[mix][s] = {"active": act, "decisive_terms": terms, "DSR": round(1 - len(act) / len(FAMILIES), 4)}
    return res


# ------------------------------------------------------------------------------------------- E11
def _spec(entity, partition, clustering, columns, family, origin, lookups=None, supersedes=None, extra=None):
    cols = sorted(set(columns) - set(partition) - {r for r, _ in clustering})
    d = {"row_entity": entity, "partition": list(partition), "clustering": [list(x) for x in clustering], "columns": cols,
         "lookups": sorted(set(lookups or [])), "family": family, "origin": origin, "supersedes": supersedes or []}
    if extra:
        d.update(extra)
    d["physical_id"] = sha256_obj({k: d[k] for k in ("row_entity", "partition", "clustering", "columns")})[:12]
    return d


def _clustering(c: dict, keys: list[str]) -> list[tuple[str, str]]:
    order_dir = {r: d for r, d in c["order"]}
    return [(r, order_dir.get(r, "asc")) for r in keys]


def specifications(ev: dict, act: dict, knobs: dict | None = None) -> list[dict]:
    knobs = {**DEFAULT_KNOBS, **(knobs or {})}
    P = []
    active_any = defaultdict(set)
    mixes_of = defaultdict(lambda: defaultdict(list))
    for mix, rows in act.items():
        for s, a in rows.items():
            for fam in a["active"]:
                active_any[s].add(fam)
                mixes_of[s][fam].append(mix)
    cat = catalog()
    for s in READ_IDS:
        c = CONTRACT[s]
        ent = c["entity"]
        t = _c1_template(s)
        part = [r for r, op, _ in c["where"] if op == "="]
        clus_keys = [k for k in t["keys"] if k not in part]
        clus = _clustering(c, clus_keys)
        allneed = t["columns"]
        ident = c["distinct"] or "id"
        for fam in sorted(active_any[s]):
            o = {"statement": s, "family": fam, "mixes": mixes_of[s][fam],
                 "evidence": {m: act[m][s]["decisive_terms"][fam] for m in mixes_of[s][fam]}}
            if fam == "C0":
                cols = [a for a in ATTR_TYPES[ent] if a != "id"]
                P.append(_spec(ent, ["id"], [], cols, fam, o))
            elif fam in ("C1", "C4"):
                P.append(_spec(ent, part, clus, allneed, fam, o))
            elif fam == "C2":
                look = [r for r in allneed if r not in clus_keys]
                keep = [r for r in allneed if "." not in r and r in clus_keys]
                P.append(_spec(ent, part, clus, keep + ([ident] if ident not in part + clus_keys else []), fam, o, lookups=look))
            elif fam == "C3":
                vol = set()
                for m in mixes_of[s][fam]:
                    vol |= set(ev["matrix"][m][s]["volatile_proj"])
                drop = [r for r in allneed if gattr(ent, r) in vol]
                P.append(_spec(ent, part, clus, [r for r in allneed if r not in drop], fam, o, lookups=drop,
                               supersedes=[{"statement": s, "family": "C1", "attributes": sorted(gattr(ent, r) for r in drop)}] if knobs["supersedes"] else []))
            elif fam == "C7":
                best, ratio = None, float("inf")
                for drop in part:
                    keep = [r for r in part if r != drop]
                    st, counts = stats()
                    rr = _expected_rows(c, keep, st, counts)
                    if rr < ratio:
                        best, ratio = (keep, drop), rr
                keep, drop = best
                P.append(_spec(ent, keep, _clustering(c, [drop] + clus_keys), allneed, fam, o,
                               supersedes=[{"statement": s, "family": "C1"}] if knobs["supersedes"] else []))
            elif fam == "C8":
                nav = [r for r in allneed if "." in r and resolve(ent, r)[0] != ev["fragments"][s]["root"]]
                fks = sorted({FK[ent][r.split(".")[0]] for r in nav})
                P.append(_spec(ent, part, clus, [r for r in allneed if r not in nav] + fks, fam, o, lookups=nav,
                               supersedes=[{"statement": s, "family": "C1", "attributes": sorted(gattr(ent, r) for r in nav)}] if knobs["supersedes"] else []))
            elif fam == "C6":
                bytarget = defaultdict(set)
                for r in ev["matrix"][mixes_of[s][fam][0]][s]["support_attrs"]:
                    e2, a2, hops = resolve(ent, r)
                    bytarget[e2].add(a2)
                for e2, attrs in sorted(bytarget.items()):
                    P.append(_spec(e2, ["id"], [], sorted(attrs - {"id"}), fam, o))
            elif fam == "C5":
                pass   # built per sharing group below
    # C5: one fused spec per sharing group (members with C5 active)
    groups = defaultdict(list)
    for s in READ_IDS:
        if "C5" in active_any[s]:
            groups[tuple(ev["matrix"][WORKLOADS[0]][s]["signature"])].append(s)
    for g, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        lead = next((m for m in members if CONTRACT[m]["limit"] is not None and ev["matrix"][WORKLOADS[0]][m]["expected_rows"] > cat["parameters"]["lambda2_rows"]), members[0])
        c = CONTRACT[lead]
        t = _c1_template(lead)
        part = [r for r, op, _ in c["where"] if op == "="]
        clus = _clustering(c, [k for k in t["keys"] if k not in part])
        cols = set()
        for m in members:
            cols |= set(_c1_template(m)["columns"]) | set(_c1_template(m)["keys"])
        o = {"statement": members, "family": "C5", "mixes": sorted({x for m in members for x in mixes_of[m]["C5"]}),
             "evidence": {m: {mx: act[mx][m]["decisive_terms"]["C5"] for mx in mixes_of[m]["C5"]} for m in members}}
        P.append(_spec(c["entity"], part, clus, sorted(cols), "C5", o,
                       supersedes=[{"statement": m, "family": "C1"} for m in members] if knobs["supersedes"] else []))
    # writes: C0 per written entity, C6 per missing maintenance key set
    for w in WRITE_IDS:
        c = CONTRACT[w]
        ent = c["entity"]
        for fam in sorted(active_any[w]):
            o = {"statement": w, "family": fam, "mixes": mixes_of[w][fam], "evidence": {m: act[m][w]["decisive_terms"][fam] for m in mixes_of[w][fam]}}
            if fam == "C0":
                P.append(_spec(ent, ["id"], [], [a for a in ATTR_TYPES[ent] if a != "id"], fam, o))
            elif fam == "C6":
                bytarget = defaultdict(set)
                for g in ev["matrix"][mixes_of[w][fam][0]][w]["missing_maintenance_keys"]:
                    e2, a2 = g.split(".")
                    bytarget[e2].add(a2)
                for e2, attrs in sorted(bytarget.items()):
                    P.append(_spec(e2, ["id"], [], sorted(attrs - {"id"}), fam, o))
    for i, p in enumerate(P):
        p["spec_id"] = f"P{i + 1:03d}"
        p["ddl"] = ddl(p, f"t_{p['spec_id'].lower()}")
    return P


# ------------------------------------------------------------------------------------------- DDL
CQL_TYPE = {"ID": "int", "Integer": "int", "Float": "double", "Date": "timestamp", "String": "text", "FK": "int"}


def colname(ref: str) -> str:
    return ref.replace(".", "__")


def coltype(entity: str, ref: str) -> str:
    e, a, _ = resolve(entity, ref)
    return CQL_TYPE[ATTR_TYPES[e][a]]


def ddl(spec: dict, name: str, keyspace: str | None = None) -> str:
    e = spec["row_entity"]
    allc = spec["partition"] + [r for r, _ in spec["clustering"]] + spec["columns"]
    cols = ", ".join(f"{colname(r)} {coltype(e, r)}" for r in allc)
    pk = "(" + ", ".join(colname(r) for r in spec["partition"]) + ")"
    ck = ", ".join(colname(r) for r, _ in spec["clustering"])
    order = ", ".join(f"{colname(r)} {d.upper()}" for r, d in spec["clustering"])
    tbl = f"{keyspace}.{name}" if keyspace else name
    q = f"CREATE TABLE {tbl} ({cols}, PRIMARY KEY ({pk}{', ' + ck if ck else ''}))"
    if spec["clustering"]:
        q += f" WITH CLUSTERING ORDER BY ({order})"
    return q


# ------------------------------------------------------------------------------------------- E12
def spec_bytes(spec: dict) -> int:
    e = spec["row_entity"]
    return sum(attr_bytes(e, r) for r in spec["partition"] + [r for r, _ in spec["clustering"]] + spec["columns"])


def canonicalize(P: list[dict], rho_max: float, knobs: dict | None = None) -> tuple[list[dict], list[dict]]:
    knobs = {**DEFAULT_KNOBS, **(knobs or {})}
    if not knobs["canonicalization"]:
        C = [dict(p, canonical_id=f"C{i + 1:03d}", members=[p["spec_id"]], origin_all=[p["origin"]], families=[p["family"]]) for i, p in enumerate(P)]
        for m in C:
            m["physical_id"] = sha256_obj({k: m[k] for k in ("row_entity", "partition", "clustering", "columns")})[:12]   # structural identity
            m["name"] = f"{canonical_name(m)}_{m['spec_id'].lower()}"
            m["ddl"] = ddl(m, m["name"])
        return C, []
    by_id = defaultdict(list)
    for p in P:
        by_id[p["physical_id"]].append(p)
    merged = []
    for pid, ps in sorted(by_id.items(), key=lambda kv: kv[1][0]["spec_id"]):
        base = dict(ps[0])
        base["members"] = [p["spec_id"] for p in ps]
        base["origin_all"] = [p["origin"] for p in ps]
        base["families"] = sorted({p["family"] for p in ps})
        base["lookups"] = sorted({x for p in ps for x in p["lookups"]})
        base["supersedes"] = [x for p in ps for x in p["supersedes"]]
        merged.append(base)
    # safe fusion: same scope (entity, partition, clustering), compatible paths, redundancy <= rho_max,
    # never re-adding an attribute that a member's family removed on purpose (C3/C8 supersedes)
    fusions, out, used = [], [], set()
    scope = defaultdict(list)
    for m in merged:
        scope[(m["row_entity"], tuple(m["partition"]), tuple(map(tuple, m["clustering"])))].append(m)
    for key, ms in scope.items():
        ms = sorted(ms, key=lambda m: m["spec_id"])
        while ms:
            a = ms.pop(0)
            for b in list(ms):
                removed = {x for s in a["supersedes"] + b["supersedes"] for x in s.get("attributes", [])}
                union = sorted(set(a["columns"]) | set(b["columns"]))
                adds = {gattr(a["row_entity"], r) for r in union} & removed
                fused = dict(a, columns=union)
                red = max((spec_bytes(fused) - spec_bytes(x)) / spec_bytes(x) for x in (a, b))
                if adds or red > rho_max:
                    continue
                fusions.append({"into": a["spec_id"], "absorbed": b["spec_id"], "redundancy": round(red, 4), "scope": [key[0], list(key[1]), [list(x) for x in key[2]]]})
                fused["members"] = a["members"] + b["members"]
                fused["origin_all"] = a["origin_all"] + b["origin_all"]
                fused["families"] = sorted(set(a["families"]) | set(b["families"]))
                fused["lookups"] = sorted(set(a["lookups"]) | set(b["lookups"]))
                fused["supersedes"] = a["supersedes"] + b["supersedes"]
                fused["physical_id"] = sha256_obj({k: fused[k] for k in ("row_entity", "partition", "clustering", "columns")})[:12]
                a = fused
                ms.remove(b)
            out.append(a)
    out.sort(key=lambda m: m["spec_id"])
    for i, m in enumerate(out):
        m["canonical_id"] = f"C{i + 1:03d}"
        m["name"] = canonical_name(m)
        m["ddl"] = ddl(m, m["name"])
    return out, fusions


def canonical_name(m: dict) -> str:
    e = m["row_entity"]
    if m["partition"] == ["id"] and not m["clustering"]:
        fam = "base" if "C0" in m["families"] else "support"
        return f"{e}_{fam}" if fam == "base" else f"{e}_{'_'.join(colname(c) for c in m['columns'])[:40]}_{m['physical_id'][:4]}"
    return f"{e}_by_{'_'.join(colname(p) for p in m['partition'])}_{m['physical_id'][:6]}"
