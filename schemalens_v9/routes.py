"""E13 — automatic route synthesis.

Given a set of canonical tables S:
  * read_options(q, S): every QueryPlan for read q (main table + SupportQuery lookups) with expected counts;
  * write_needs(w, t): what write w must do on table t (writes, support reads, or "unmaintainable");
  * evaluate(S): picks, per read, the cheapest option under a cost function, builds the WritePlans of the 8
    writes over every table of S, and validates (each read has a route; every table holding written data is
    in the corresponding WritePlan).
Counts are expectations at full scale from E4 (rows per partition), the method's design statistics.
"""
from __future__ import annotations

import itertools
from collections import defaultdict

from .contract import CONTRACT, READ_IDS, WRITE_IDS
from .phase2 import _expected_rows, gattr, needed_refs, resolve, stats
from .schema_map import FK, REL


def _avail(t: dict) -> set[str]:
    return set(t["partition"]) | {r for r, _ in t["clustering"]} | set(t["columns"])


def _lookup_tables(S: list[dict], entity: str, attrs: set[str]) -> list[dict]:
    return [t for t in S if t["row_entity"] == entity and t["partition"] == ["id"] and not t["clustering"] and attrs <= set(t["columns"])]


def _fk_present(entity: str, rel: str, avail: set[str]) -> bool:
    return FK[entity].get(rel) in avail or f"{rel}.id" in avail


def read_options(q: int, S: list[dict]) -> list[dict]:
    c = CONTRACT[q]
    ent = c["entity"]
    st, counts = stats()
    eq = [r for r, op, _ in c["where"] if op == "="]
    rg = [r for r, op, _ in c["where"] if op != "="]
    need = needed_refs(c)
    used_in_logic = set(rg) | {r for r, _ in c["order"]} | ({c["distinct"]} if c["distinct"] else set())
    rng = st["range_selectivity"]["items.end_date >= 2010-01-01 (since fixo da carga)"]
    full_rows = _expected_rows(c, eq, st, counts) * (rng if rg else 1.0)
    opts = []
    for t in S:
        if t["row_entity"] != ent or not t["partition"]:
            continue
        if not set(t["partition"]) <= set(eq):
            continue
        avail = _avail(t)
        dropped = [r for r in eq if r not in t["partition"]]
        if any(r not in avail for r in dropped):
            continue
        # identity: a row of the result must be identifiable (id or the distinct key)
        ident = c["distinct"] or "id"
        missing = [r for r in need if r not in avail and r not in eq]
        groups = defaultdict(set)
        ok = True
        for r in missing:
            e2, a2, hops = resolve(ent, r)
            if not hops:
                if ident != "id" or "id" not in avail:
                    ok = False
                    break
                groups[(ent, None)].add(a2)
            elif len(hops) == 1:
                rel = hops[0][1]
                if a2 == "id" and _fk_present(ent, rel, avail):
                    continue
                if not _fk_present(ent, rel, avail):
                    ok = False
                    break
                groups[(e2, rel)].add(a2)
            else:
                ok = False
                break
        if not ok:
            continue
        lookup_choices = []
        for (e2, rel), attrs in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")):
            cands = _lookup_tables(S, e2, attrs)
            if not cands:
                ok = False
                break
            lookup_choices.append([(e2, rel, sorted(attrs), x["canonical_id"]) for x in cands])
        if not ok:
            continue
        part_rows = _expected_rows(c, t["partition"], st, counts)
        clus = [r for r, _ in t["clustering"]]
        range_push = bool(rg) and clus[:1] == rg[:1] and not dropped
        main_rows = part_rows * (rng if range_push else 1.0)
        order = [(r, d) for r, d in c["order"]]
        tail = [(r, d) for r, d in map(tuple, t["clustering"])]
        # server-side LIMIT only when the clustering order IS the contract order (with its tie-break) and nothing is post-filtered
        exact = bool((not dropped) and c["limit"] is not None and (tail == order or tail == order + [(ident, "asc")]))
        if exact and not any(r in used_in_logic for r in missing):
            main_read = min(main_rows, c["limit"])
        else:
            main_read = main_rows
        returned = min(full_rows, c["limit"]) if c["limit"] is not None else full_rows
        for combo in itertools.product(*lookup_choices) if lookup_choices else [()]:
            per_row = len(combo)
            lk_rows = main_read if any(r in used_in_logic for r in missing) else returned
            if c["distinct"]:
                lk_rows = min(lk_rows, returned) if not any(r in used_in_logic for r in missing) else lk_rows
            reads = 1 + lk_rows * per_row
            opts.append({"statement": q, "main": t["canonical_id"], "lookups": [list(x) for x in combo],
                         "relaxed_drop": dropped, "range_pushdown": range_push, "limit_pushdown": exact,
                         "reads": round(reads, 4), "rows": round(main_read + lk_rows * per_row, 4), "writes": 0,
                         "rows_main": round(main_read, 4), "rows_lookup_each": round(lk_rows, 4)})
    return opts


def write_needs(w: int, t: dict) -> dict:
    c = CONTRACT[w]
    ent = c["entity"]
    keys = t["partition"] + [r for r, _ in t["clustering"]]
    allr = keys + t["columns"]
    if c["op"] == "INSERT":
        if t["row_entity"] != ent:
            return {"affected": False}
        sup = defaultdict(set)
        for r in allr:
            e2, a2, hops = resolve(ent, r)
            if not hops:
                if a2 not in c["values"]:
                    return {"affected": True, "maintainable": False, "reason": f"{ent}.{a2} não vem na inserção"}
            elif len(hops) == 1:
                if a2 == "id":
                    continue
                sup[e2].add(a2)
            else:
                # two hops (e.g. seller.region_id): first hop FK is a parameter; read the second FK from the target
                first_to = hops[0][2]
                sup[first_to].add(FK[first_to][hops[1][1]] if a2 == "id" else hops[1][1] + "." + a2)
                if a2 != "id":
                    return {"affected": True, "maintainable": False, "reason": f"navegação de dois saltos com atributo {r}"}
        return {"affected": True, "maintainable": True, "writes": 1, "support": {k: sorted(v) for k, v in sup.items()}}
    # UPDATE
    changed = set(c["values"])
    if t["row_entity"] == ent:
        own = {resolve(ent, r)[1] for r in allr if "." not in r}
        if not (own & changed):
            return {"affected": False}
        key_changed = {resolve(ent, r)[1] for r in keys if "." not in r} & changed
        sup = defaultdict(set)
        for r in keys:
            e2, a2, hops = resolve(ent, r)
            if not hops and a2 != "id":
                sup[ent].add(a2)
            elif hops:
                return {"affected": True, "maintainable": False, "reason": f"chave navegada {r} em tabela atualizada"}
        if key_changed:
            return {"affected": True, "maintainable": False, "reason": f"atualização de atributo-chave {sorted(key_changed)}"}
        return {"affected": True, "maintainable": True, "writes": 1, "support": {k: sorted(v) for k, v in sup.items()}}
    # copies of the updated entity's attributes through navigation
    for r in allr:
        e2, a2, hops = resolve(t["row_entity"], r)
        if hops and e2 == ent and a2 in changed:
            return {"affected": True, "maintainable": False, "reason": f"cópia navegada de {ent}.{a2} sem índice reverso"}
    return {"affected": False}


def evaluate(S: list[dict], cost=lambda o: o["reads"] + 0.0 * o["rows"]) -> dict:
    """Route every statement over S. Returns plans, validity and per-statement counts."""
    by_id = {t["canonical_id"]: t for t in S}
    plans, invalid = {}, []
    for q in READ_IDS:
        opts = read_options(q, S)
        if not opts:
            invalid.append(f"leitura {q} sem rota")
            continue
        plans[q] = min(opts, key=lambda o: (cost(o), o["main"], str(o["lookups"])))
    for w in WRITE_IDS:
        tables, sup = [], defaultdict(set)
        for t in S:
            n = write_needs(w, t)
            if not n["affected"]:
                continue
            if not n["maintainable"]:
                invalid.append(f"escrita {w}: tabela {t['canonical_id']} não mantível ({n['reason']})")
                continue
            tables.append({"table": t["canonical_id"], "writes": n["writes"]})
            for e2, attrs in n["support"].items():
                sup[e2] |= set(attrs)
        sup_reads = []
        for e2, attrs in sorted(sup.items()):
            L = _lookup_tables(S, e2, attrs)
            if L:
                sup_reads.append({"entity": e2, "attrs": sorted(attrs), "table": min(L, key=lambda x: (len(x["columns"]), x["canonical_id"]))["canonical_id"]})
            else:   # split per attribute if no single table covers all
                for a in sorted(attrs):
                    L = _lookup_tables(S, e2, {a})
                    if not L:
                        invalid.append(f"escrita {w}: sem tabela de apoio para {e2}.{a}")
                        continue
                    sup_reads.append({"entity": e2, "attrs": [a], "table": min(L, key=lambda x: (len(x["columns"]), x["canonical_id"]))["canonical_id"]})
        c = CONTRACT[w]
        # the written entity must be stored somewhere (its own data cannot be lost)
        if not tables:
            invalid.append(f"escrita {w}: nenhuma tabela guarda {c['entity']}")
        plans[w] = {"statement": w, "tables": tables, "support": sup_reads, "writes": sum(x["writes"] for x in tables),
                    "reads": len(sup_reads), "rows": len(sup_reads)}
    # every table holding data written by w is in w's plan (checked by construction above; recorded)
    return {"plans": plans, "invalid": invalid, "valid": not invalid, "tables": sorted(by_id)}
