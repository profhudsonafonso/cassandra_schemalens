"""Fase 6: E20 (benchmark na coorte, protocolo congelado) e E21 (análise).

The request loop is the V8-012 frozen protocol (reused/methodology/protocol_012.run_protocol): 4 workloads ×
5 repetitions × (1,000 warm-up + 10,000 measured), seeded histories, reset_state before and after every
repetition.  It is re-implemented here line-for-line only so that raw files are written under the V9 run
(the reused function writes paths relative to its own repository) and so that progress reaches the monitor.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import statistics
import time
from collections import defaultdict
from typing import Any

from . import isolation
from . import phase5 as M
from . import phase5_run as PR
from .common import OLD_RUN_011, RUN, now_iso, read_json, write_json
from .stages import incident, load_state, save_state, set_stage, stage_dir

WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
MEASURED, WARMUP, REPS = 10_000, 1_000, 5
OUT = RUN / "stages/E20_benchmark"
RAW = OUT / "raw"
T975 = {4: 2.776}
IDLE_S = 600


def order() -> list[str]:
    H, _ = PR.h_schemas()
    ids = [h["schema_id"] for h in H]
    ext = ["NoSE", "V8_AUTO_H_norm", "QPG"]
    seq = []
    for i, h in enumerate(ids):     # interleave generated and external deployments
        seq.append(h)
        if i < len(ext):
            seq.append(ext[i])
    return seq


def run_protocol(adapter, dep: str, ks: str, label_i: int, n_dep: int, stage: str = "E20", raw_dir=None) -> dict:
    from reused.methodology.protocol_012 import stats
    from reused.methodology.semantic_replay_011 import canonical, hash_value
    from reused.methodology.workload_005 import histories
    from .phase1 import _benchmark_domain
    dom = _benchmark_domain()
    raw_dir = raw_dir or RAW
    raw_dir.mkdir(parents=True, exist_ok=True)
    summaries, raw_files = [], []
    adapter.reset_state()
    for workload in WORKLOADS:
        wrecs = []
        for rep in range(REPS):
            adapter.reset_state()
            for req in histories(workload, rep, WARMUP, dom, "v9", phase="warmup"):
                ctx = {}
                for sid in req["constituent_statement_ids"]:
                    adapter.execute_statement(sid, req["parameters"], ctx)
            recs = []
            for k, req in enumerate(histories(workload, rep, MEASURED, dom, "v9", phase="measured")):
                ctx, srows, err = {}, [], None
                start = time.perf_counter_ns()
                try:
                    for sid in req["constituent_statement_ids"]:
                        r = adapter.execute_statement(sid, req["parameters"], ctx)
                        srows.append({"statement_id": sid, "logical_rows": canonical(r["logical_rows"]), "canonical_hash": hash_value(r["logical_rows"]),
                                      "row_count": r["row_count"], "physical_reads": r["physical_reads"], "physical_writes": r["physical_writes"],
                                      "support_reads": r["support_reads"], "rows_fetched": r["rows_fetched"]})
                except Exception as exc:
                    err = repr(exc)
                lat = (time.perf_counter_ns() - start) / 1e6
                row = {"logical_request_id": req["logical_request_id"], "deployment_id": dep, "keyspace": ks, "workload": workload,
                       "repetition": rep, "request_group": req["request_group"], "statements": srows, "latency_ms": lat,
                       "physical_reads": sum(x["physical_reads"] for x in srows), "physical_writes": sum(x["physical_writes"] for x in srows),
                       "support_reads": sum(x["support_reads"] for x in srows), "rows_fetched": sum(x["rows_fetched"] for x in srows),
                       "rows_returned": sum(x["row_count"] for x in srows), "error": err}
                recs.append(row)
                if k % 1000 == 999:
                    set_stage(stage, progress={"text": f"esquema {label_i + 1} de {n_dep} ({dep}), carga {workload}, repetição {rep + 1} de {REPS}, {k + 1:,} de {MEASURED:,} requisições".replace(",", "."),
                                               "done": ((label_i * 4 + WORKLOADS.index(workload)) * REPS + rep) * MEASURED + k + 1, "total": n_dep * 4 * REPS * MEASURED})
            wrecs.extend(recs)
            adapter.reset_state()
            s = stats(recs)
            s.update({"deployment_id": dep, "workload": workload, "repetition": rep})
            summaries.append(s)
        payload = b"\n".join(json.dumps(r, sort_keys=True, default=str).encode() for r in wrecs) + b"\n"
        comp = gzip.compress(payload, mtime=0)
        path = raw_dir / f"{dep}_{workload}.jsonl.gz"
        path.write_bytes(comp)
        raw_files.append({"path": str(path), "records": len(wrecs), "sha256": hashlib.sha256(comp).hexdigest()})
    adapter.reset_state()
    return {"summaries": summaries, "raw_files": raw_files}


def _pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def reference_hashes() -> dict:
    from reused.methodology.semantic_replay_011 import hash_value
    from reused.methodology.oracle_011 import decanonical
    ref = {}
    for w in WORKLOADS:
        with gzip.open(OLD_RUN_011 / f"full_scale_matched_cohort/raw/AUTO_H_norm_{w}.jsonl.gz", "rt") as fh:
            for line in fh:
                r = json.loads(line)
                for st in r["statements"]:
                    ref[(w, r["repetition"], r["logical_request_id"], st["statement_id"])] = st["canonical_hash"]
    return ref


def agreement(raw_files, ref) -> dict:
    n = agree = 0
    first = []
    for f in raw_files:
        with gzip.open(f["path"], "rt") as fh:
            for line in fh:
                r = json.loads(line)
                for st in r["statements"]:
                    n += 1
                    ok = ref.get((r["workload"], r["repetition"], r["logical_request_id"], st["statement_id"])) == st["canonical_hash"]
                    agree += ok
                    if not ok and len(first) < 10:
                        first.append({"request": r["logical_request_id"], "sid": st["statement_id"]})
    return {"statement_results": n, "agree": agree, "status": "PASS" if n and agree == n else "FAIL", "first_disagreements": first}


def wait_idle():
    t0 = None
    while True:
        snap = isolation.snapshot()
        st = load_state()
        st.setdefault("idle_wait", {}).update({"last": snap})
        save_state(st)
        if snap["idle"]:
            t0 = t0 or time.time()
            set_stage("E20", progress={"text": f"aguardando nó ocioso: {int(time.time() - t0)} de {IDLE_S} s", "done": 0, "total": 1})
            if time.time() - t0 >= IDLE_S:
                return snap
        else:
            t0 = None
            set_stage("E20", progress={"text": f"nó ocupado ({len(snap['bulk_writers'])} escritor(es), compactações {snap['pending_compactions']}); aguardando", "done": 0, "total": 1})
        time.sleep(15)


def adapter_for(dep, s, state, closure):
    from reused.methodology.semantic_replay_011 import DeploymentAdapter008
    if dep in PR.EXTERNAL:
        ks, kind = PR.EXTERNAL[dep]
        return DeploymentAdapter008(s, dep, ks, kind, closure), ks
    H, C = PR.h_schemas()
    h = next(x for x in H if x["schema_id"] == dep)
    ks = PR.ks_of(dep)
    return M.GeneratedAdapter(s, ks, h, {t: C[t] for t in h["canonical_ids"]}, state), ks


def ks_fingerprint(s, ks):
    out = {}
    for r in s.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", (ks,)):
        rows = list(s.execute(f"SELECT * FROM {ks}.{r['table_name']}"))
        out[r["table_name"]] = M.canon_rows([dict(x) for x in rows])
    return out


def e20():
    from reused.methodology.semantic_replay_011 import source_closure
    if not (read_json(RUN / "stages/E18_correctness/RESULT.json", {}) or {}).get("status") == "PASS":
        raise RuntimeError("E18 não passou: benchmark bloqueado")
    if not (read_json(RUN / "stages/E19_baselines/RESULT.json", {}) or {}).get("status") == "PASS":
        raise RuntimeError("E19 não passou: benchmark bloqueado")
    frozen = read_json(RUN / "stages/E16_hbench_frozen/HBENCH_FROZEN.json")
    raw, ids, state = PR.cohort()
    ref = reference_hashes()
    seq = order()
    partial = {}
    cl, s = M.session()
    try:
        closure, _ = source_closure(s)
        first = True   # every (re)start of the window waits 10 min of idle node before the first measurement
        for i, dep in enumerate(seq):
            res_path = OUT / f"{dep}_RESULT.json"
            if (read_json(res_path, {}) or {}).get("status") == "PASS":
                partial[dep] = read_json(res_path)["latency_mean_ms"]
                continue
            node = wait_idle() if first or not isolation.snapshot()["idle"] else isolation.snapshot()
            first = False
            ad, ks = adapter_for(dep, s, state, closure)
            before = ks_fingerprint(s, ks)
            started = now_iso()
            with isolation.BulkWriterWatch() as watch:
                res = run_protocol(ad, dep, ks, i, len(seq))
            iso = watch.report()
            after = ks_fingerprint(s, ks)
            changed = sorted(t for t in before if before[t] != after.get(t))
            agr = agreement(res["raw_files"], ref)
            errors = sum(x["errors"] for x in res["summaries"])
            lat = {w: statistics.fmean(x["mean_ms"] for x in res["summaries"] if x["workload"] == w) for w in WORKLOADS}
            status = "PASS" if errors == 0 and agr["status"] == "PASS" and not changed else "FAIL"
            if status == "PASS" and not iso["clean"]:
                status = "INVALID_CONCURRENT_LOAD"
            rec = {"deployment_id": dep, "keyspace": ks, "status": status, "started_at": started, "finished_at": now_iso(),
                   "node_state_at_start": node, "isolation": iso, "state_restoration": {"tables_changed": changed}, "agreement_with_v8_reference": agr,
                   "errors": errors, "protocol": {"workloads": WORKLOADS, "repetitions": REPS, "warmup": WARMUP, "measured": MEASURED},
                   "summaries": res["summaries"], "raw_files": res["raw_files"], "latency_mean_ms": lat, "hbench_frozen_sha256": frozen["content_sha256"]}
            write_json(res_path, rec)
            partial[dep] = lat
            write_json(OUT / "PARTIAL.json", {"note": "PARCIAL — latência média (ms) por esquema e carga, antes da aceitação", "latency_mean_ms": partial})
            if status == "INVALID_CONCURRENT_LOAD":
                st = load_state()
                st.setdefault("invalidated_measurements", []).append({"deployment": dep, "at": now_iso()})
                save_state(st)
                incident(f"{dep}: medição invalidada por carga concorrente; será refeita")
                res_path.rename(res_path.with_name(f"{dep}_INVALID_{int(time.time())}.json"))
                return "FAIL", [], {"invalidated": dep}, {}
            if status != "PASS":
                return "FAIL", [res_path], {"failed": dep, "agreement": agr["status"], "errors": errors, "restoration": changed}, {}
    finally:
        cl.shutdown()
    outs = [OUT / f"{d}_RESULT.json" for d in seq]
    return "PASS", outs, {"deployments": seq, "all_pass": True}, {}
