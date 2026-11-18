"""Indexed mock fetcher for Stage 9 smoke tests.

Accelerates the existing Stage 6 mock read without changing any upstream file.
This index exists only in memory and says NOTHING about Cassandra indexing.
"""
from __future__ import annotations
from collections import defaultdict


class IndexedReferenceFetcher:
    def __init__(self, materializer):
        self.plans = materializer.plans
        self.by_partition = {}
        for cid, plan in materializer.plans.items():
            pk = plan["partition_key"]
            idx = defaultdict(list)
            for row in materializer.rows[cid].values():
                idx[tuple(row[k] for k in pk)].append(row)
            for rows in idx.values():
                ck = plan["clustering_key"]
                rows.sort(key=lambda r: tuple(r[k] for k in ck),
                          reverse=cid in {"ts0_metric_raw", "ts5_global_max_minute"})
            self.by_partition[cid] = idx

    def __call__(self, req):
        cid = req["candidate_id"]
        plan = self.plans[cid]
        plen = len(plan["partition_key"])
        params = req["parameters"]
        matches = self.by_partition[cid].get(tuple(params[:plen]), [])
        rest = params[plen:]
        if len(rest) == 2:
            lo, hi = rest
            col = plan["clustering_key"][0]
            matches = [r for r in matches if lo <= r[col] < hi]
        elif len(rest) == 1:
            hi = rest[0]
            col = plan["clustering_key"][0]
            matches = [r for r in matches if r[col] < hi]
        elif rest:
            raise ValueError("Unexpected Stage 5 CQL predicate")
        if "LIMIT 1" in req["cql"]:
            matches = matches[:1]
        elif "LIMIT 5" in req["cql"]:
            matches = matches[:5]
        return [row.copy() for row in matches]
