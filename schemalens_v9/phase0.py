"""Fase 0: E1 (preflight) and E2 (source data). Read-only on Cassandra and on the old repository."""
from __future__ import annotations

import argparse
import json
import re
import subprocess

from . import isolation
from .common import CONTAINER, KEYSPACE_PREFIX, RUN, SOURCE_KEYSPACE, now_iso, read_json, write_json
from .stages import run_stage, set_stage, stage_dir

EXPECTED_BASE = {"categories": 500, "regions": 50, "users": 200_000, "items": 2_000_000, "bids": 20_000_000,
                 "comments": 10_000_000, "buynow": 2_000_000}            # from the task statement (D-001 dataset)
EXPECTED_BASE_TOTAL = 34_200_550
EXPECTED_ALL_TOTAL = 70_400_550
EXPECTED_TABLES = 13


def _sh(cmd: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _session():
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    c = Cluster(["127.0.0.1"])
    s = c.connect()
    s.row_factory = dict_factory
    s.default_timeout = 120
    return c, s


# ------------------------------------------------------------------------------------------------ E1
def e1():
    out = stage_dir("E1") / "PREFLIGHT.json"
    ver = _sh(["docker", "exec", CONTAINER, "nodetool", "version"])
    release = (re.search(r"ReleaseVersion:\s*(\S+)", ver.stdout) or [None, None])[1]
    cqlv = _sh(["docker", "exec", CONTAINER, "cqlsh", "-e", "SELECT release_version, cql_version FROM system.local"])
    info = _sh(["docker", "exec", CONTAINER, "nodetool", "info"])
    image = _sh(["docker", "inspect", "-f", "{{.Config.Image}}", CONTAINER]).stdout.strip()
    data_df = _sh(["docker", "exec", CONTAINER, "df", "-Pk", "/var/lib/cassandra"]).stdout.splitlines()[-1].split()
    mem = _sh(["docker", "exec", CONTAINER, "sh", "-c", "grep -E '^(MemTotal|MemAvailable|Cached|Mapped):' /proc/meminfo"]).stdout
    cluster, s = _session()
    try:
        ks = sorted(r["keyspace_name"] for r in s.execute("SELECT keyspace_name FROM system_schema.keyspaces"))
        dam = None
    finally:
        cluster.shutdown()
    logs = _sh(["docker", "logs", CONTAINER], timeout=120)
    m = re.findall(r"DiskAccessMode '(\w+)' determined to be (\w+)", logs.stdout + logs.stderr)
    dam = m[-1] if m else None
    node = isolation.node_status()
    iso = isolation.snapshot()
    disk = isolation.disk()
    pre = {"checked_at": now_iso(), "cassandra_release_version_nodetool": release, "cqlsh_system_local": cqlv.stdout.strip(),
           "container": CONTAINER, "image": image, "node_status": node, "nodetool_info": info.stdout.strip().splitlines(),
           "disk_access_mode_from_log": dam, "container_meminfo": mem.strip().splitlines(),
           "container_data_volume_df_kb": {"size": int(data_df[1]), "used": int(data_df[2]), "avail": int(data_df[3]), "mount": data_df[5]},
           "host_disk": disk, "isolation_guard": iso,
           "keyspaces": ks, "keyspace_count": len(ks), "v9_keyspaces_existing": [k for k in ks if k.startswith(KEYSPACE_PREFIX)],
           "source_keyspace_present": SOURCE_KEYSPACE in ks}
    write_json(out, pre)
    checks = {"cassandra_is_3_11_11": release == "3.11.11", "node_up_normal": node["up_normal"],
              "rubis_full_present": pre["source_keyspace_present"], "no_v9_keyspace_preexisting": not pre["v9_keyspaces_existing"],
              "isolation_guard_ran": iso["checked_at"] is not None, "container_data_free_gb": round(int(data_df[3]) / 1e6, 1)}
    status = "PASS" if all(v for k, v in checks.items() if isinstance(v, bool)) else "FAIL"
    return status, [out], checks, {"isolation_at_check": iso}


# ------------------------------------------------------------------------------------------------ E2
def _pk_columns(s, table: str) -> list[str]:
    rows = s.execute("SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s", (SOURCE_KEYSPACE, table))
    return [r["column_name"] for r in sorted((r for r in rows if r["kind"] == "partition_key"), key=lambda r: r["position"])]


def _count(s, table: str, slices: int, progress) -> int:
    from cassandra.concurrent import execute_concurrent_with_args
    pk = ", ".join(_pk_columns(s, table))
    lo, hi = -(2 ** 63), 2 ** 63 - 1
    step = (hi - lo) // slices
    bounds = [(lo + i * step, (lo + (i + 1) * step) if i < slices - 1 else hi) for i in range(slices)]
    first = s.prepare(f"SELECT count(*) AS n FROM {SOURCE_KEYSPACE}.{table} WHERE token({pk}) >= ? AND token({pk}) <= ?")
    rest = s.prepare(f"SELECT count(*) AS n FROM {SOURCE_KEYSPACE}.{table} WHERE token({pk}) > ? AND token({pk}) <= ?")
    total, done = 0, 0
    chunk = 64
    for i in range(0, len(bounds), chunk):
        part = bounds[i:i + chunk]
        args0 = [b for j, b in enumerate(part) if i + j == 0]
        argsr = [b for j, b in enumerate(part) if i + j != 0]
        for stmt, args in ((first, args0), (rest, argsr)):
            if not args:
                continue
            for ok, res in execute_concurrent_with_args(s, stmt, args, concurrency=8, raise_on_first_error=True):
                total += res.one()["n"]
        done += len(part)
        progress(done)
    return total


def e2():
    cluster, s = _session()
    s.default_timeout = 600
    try:
        tables = sorted(r["table_name"] for r in s.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", (SOURCE_KEYSPACE,)))
        counts, n_tables = {}, len(tables)
        for k, t in enumerate(tables):
            slices = 2048 if t in ("bids", "bids_by_item", "comments", "comments_by_to_user") else 512

            def prog(done, _t=t, _k=k, _sl=slices):
                set_stage("E2", progress={"text": f"tabela {_k + 1} de {n_tables} ({_t}): faixa {done} de {_sl}", "done": _k * 1000 + int(1000 * done / _sl), "total": n_tables * 1000})
            counts[t] = _count(s, t, slices, prog)
    finally:
        cluster.shutdown()
    base = {t: counts.get(t) for t in EXPECTED_BASE}
    out = stage_dir("E2") / "SOURCE_COUNTS.json"
    rec = {"keyspace": SOURCE_KEYSPACE, "regenerated": False, "seed_decision": "D-001 (inherited from V8): seed 42, not regenerated",
           "method": "token-range split count(*) per table (read-only)", "counted_at": now_iso(),
           "tables": counts, "table_count": len(counts), "base_counts": base, "base_total": sum(v or 0 for v in base.values()),
           "all_total": sum(counts.values()),
           "expected": {"base": EXPECTED_BASE, "base_total": EXPECTED_BASE_TOTAL, "all_total": EXPECTED_ALL_TOTAL, "tables": EXPECTED_TABLES}}
    rec["divergences"] = [f"{t}: contado {base[t]}, esperado {EXPECTED_BASE[t]}" for t in EXPECTED_BASE if base[t] != EXPECTED_BASE[t]]
    if rec["base_total"] != EXPECTED_BASE_TOTAL:
        rec["divergences"].append(f"total base: {rec['base_total']} vs {EXPECTED_BASE_TOTAL}")
    if rec["all_total"] != EXPECTED_ALL_TOTAL:
        rec["divergences"].append(f"total geral: {rec['all_total']} vs {EXPECTED_ALL_TOTAL}")
    if len(counts) != EXPECTED_TABLES:
        rec["divergences"].append(f"tabelas: {len(counts)} vs {EXPECTED_TABLES}")
    write_json(out, rec)
    checks = {"tables_13": len(counts) == EXPECTED_TABLES, "base_total_matches": rec["base_total"] == EXPECTED_BASE_TOTAL,
              "all_total_matches": rec["all_total"] == EXPECTED_ALL_TOTAL, "divergences": len(rec["divergences"])}
    # divergence is reported, not corrected: the stage still records PASS/FAIL of the check honestly
    status = "PASS" if not rec["divergences"] else "FAIL"
    return status, [out], checks, {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=("E1", "E2"))
    a = ap.parse_args()
    r = run_stage(a.stage, {"E1": e1, "E2": e2}[a.stage])
    print(json.dumps({"stage": a.stage, "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False))
    return 0 if r["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
