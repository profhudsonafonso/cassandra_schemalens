"""Matched-scale benchmark of the four historical baselines (V8-012).

The frozen SchemaLens AUTO_H measurement ran on matched-cohort keyspaces
(5,906 items / 5,616 users / 5,690 bids).  ``baseline_benchmark_011`` measures
the baselines on full historical keyspaces (2M items / 20M bids); latencies
from different data volumes are not a valid head-to-head comparison.  This
module measures the baselines on the matched cohort that the six-deployment
semantic replay already validated, using the identical 400,000-request
protocol, a per-request paired oracle and a state-restoration check.

Frozen keyspaces are not touched: each baseline is cloned to a dedicated
``rubis_v8_c012_cohort_*`` keyspace first.
"""
from __future__ import annotations

import argparse
import json

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from . import protocol_012 as P
from .semantic_replay_011 import DeploymentAdapter008, source_closure

OUT = P.RUN_DIR / "cohort_baselines"
BASELINES = (
    ("NoSE", "rubis_v8_c005_nose_bidding", "nose_bidding"),
    ("QPG", "rubis_v8_c005_qpg_bidding", "qpg_bidding"),
    ("Normalized", "rubis_v8_c005_normalized", "normalized"),
    ("Expert", "rubis_v8_c008_expert", "expert"),
)


def target_keyspace(kind: str) -> str:
    return f"rubis_v8_c012_cohort_{kind}"


def run(only: list[str], *, label: str, measured: int, warmup: int, repetitions: int, allow_busy: bool) -> int:
    node = P.require_idle_node(allow_busy)
    out = OUT / label
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.row_factory = dict_factory
    session.default_timeout = 180
    reference = P.load_reference_hashes()
    try:
        closure, source_state_id = source_closure(session)
        ledger = P.read_json(out / "RUN_LEDGER.json", {"baselines": {}})
        for deployment, source_ks, kind in BASELINES:
            if only and kind not in only:
                continue
            if ledger["baselines"].get(kind, {}).get("status") == "PASS":
                continue
            ks = target_keyspace(kind)
            mat = P.clone_keyspace(cluster, session, source_ks, ks)
            P.write_json(OUT / "materialization" / f"{kind}.json", mat)
            adapter = DeploymentAdapter008(session, deployment, ks, kind, closure)
            before = mat["fingerprint"]
            started = P.now_iso()
            with P.BulkWriterWatch() as watch:
                result = P.run_protocol(adapter, deployment, ks, source_state_id, out / "raw",
                                        measured=measured, warmup=warmup, repetitions=repetitions)
            finished = P.now_iso()
            isolation = watch.report()
            restoration = P.state_restoration(before, P.keyspace_fingerprint(session, ks))
            oracle = P.paired_oracle([P.ROOT / f["path"] for f in result["raw_files"]], reference)
            storage = P.keyspace_storage(ks)
            errors = sum(s["errors"] for s in result["summaries"])
            status = "PASS" if oracle["status"] == "PASS" and errors == 0 and restoration["status"] == "PASS" else "FAIL"
            if status == "PASS" and not allow_busy and not isolation["clean"]:
                status = "INVALID_CONCURRENT_LOAD"
            record = {"deployment_id": deployment, "kind": kind, "status": status, "source_keyspace": source_ks, "keyspace": ks,
                      "started_at": started, "finished_at": finished, "node_state": node, "isolation": isolation,
                      "state_restoration": restoration, "materialization": {k: mat[k] for k in ("tables", "ddl_sha256", "acknowledged_mutations")},
                      "protocol": {k: result[k] for k in ("measured_requests_per_repetition", "warmup_requests_per_repetition", "repetitions")},
                      "summaries": result["summaries"], "raw_files": result["raw_files"], "oracle": oracle, "storage": storage, "errors": errors,
                      "scale": "matched cohort (same keyspace scale as the frozen AUTO_H measurement)"}
            P.write_json(out / f"{kind}_RESULT.json", record)
            ledger["baselines"][kind] = {"status": status, "result": str((out / f"{kind}_RESULT.json").relative_to(P.ROOT))}
            P.write_json(out / "RUN_LEDGER.json", ledger)
            print(json.dumps({"baseline": kind, "status": status, "oracle": {k: oracle[k] for k in ("observations", "equal", "mismatch", "errors")}}), flush=True)
            if status != "PASS":
                return 1
        return 0
    finally:
        cluster.shutdown()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--label", default="protocol")
    ap.add_argument("--measured", type=int, default=P.MEASURED)
    ap.add_argument("--warmup", type=int, default=P.WARMUP)
    ap.add_argument("--repetitions", type=int, default=P.REPETITIONS)
    ap.add_argument("--allow-busy", action="store_true", help="correctness pilot only; timings are not evidence")
    a = ap.parse_args()
    if a.label == "protocol" and (a.allow_busy or (a.measured, a.warmup, a.repetitions) != (P.MEASURED, P.WARMUP, P.REPETITIONS)):
        raise SystemExit("the 'protocol' label is reserved for the frozen protocol on an idle node")
    return run([x for x in a.only.split(",") if x], label=a.label, measured=a.measured, warmup=a.warmup,
               repetitions=a.repetitions, allow_busy=a.allow_busy)


if __name__ == "__main__":
    raise SystemExit(main())
