"""Instantiate TSBS CassandraCandidateSpec objects from PRECOMPUTED TS0-TS8 sets.

No cost model, performance results, C0-C9 families, or query-ID-specific rules
are used. Source kind from query_manifest.csv determines which TS5 materialized
aggregate *shape* is appropriate, AFTER TS5 was independently activated.

CQL schemas are concrete, but load/write plans and query coverage are NOT
claimed. Query templates are single-table primitives for a later QueryPlan.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

TS_FAMILIES = frozenset(f"TS{i}" for i in range(9))
TS_MODIFIERS = frozenset(f"M{i}" for i in range(1, 6))

# Exact metric fields of the TSBS cpu-only generator (upstream schema JSON).
TYPE_BY_COLUMN = {"hostname": "text", "metric_name": "text", "bucket_day": "date",
                  "timestamp_ns": "bigint", "minute_ns": "bigint", "hour_ns": "bigint",
                  "value": "double", "max_value": "double", "value_sum": "double",
                  "sample_count": "bigint"}


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class CandidateSpec:
    """One independently materializable physical TABLE option, not a deployment."""

    candidate_id: str
    family: str
    variant: str
    table_name: str
    columns: dict[str, str]
    partition_key: tuple[str, ...]
    clustering_key: tuple[str, ...]
    clustering_order: tuple[str, ...]
    purpose: str
    read_cql_template: str
    client_steps: tuple[str, ...]
    producer_required: str
    chosen_modifiers: dict[str, Any] = field(default_factory=dict)
    schema_only: bool = True
    complete_query_plan: bool = False
    complete_write_plan: bool = False
    benchmarked: bool = False

    def validate(self) -> None:
        if self.family not in TS_FAMILIES:
            raise ValueError(f"Unknown TS-only family: {self.family}")
        if not self.partition_key or not self.clustering_key:
            raise ValueError(f"Missing PK or CK for {self.candidate_id}")
        if set(self.partition_key) & set(self.clustering_key):
            raise ValueError(f"Partition/clustering key collision in {self.candidate_id}")
        if not (set(self.partition_key) | set(self.clustering_key)) <= set(self.columns):
            raise ValueError(f"Key columns missing from definition: {self.candidate_id}")
        if len(self.clustering_order) != len(self.clustering_key):
            raise ValueError(f"Missing CK order in {self.candidate_id}")
        if any(o not in {"ASC", "DESC"} for o in self.clustering_order):
            raise ValueError(f"Bad CK order in {self.candidate_id}")
        if "ALLOW FILTERING" in self.read_cql_template.upper():
            raise ValueError("This experimental policy forbids ALLOW FILTERING")
        if any(("(" in name or ")" in name or ";" in name) for name in self.columns):
            raise ValueError("Invalid CQL column identifier")

    def ddl(self, keyspace: str | None = None) -> str:
        """CQL CREATE TABLE with an explicit compound PK and clustering order."""
        self.validate()
        prefix = f"{keyspace}." if keyspace else ""
        definitions = [f"    {name} {type_name}" for name, type_name in self.columns.items()]
        pk = "(" + ", ".join(self.partition_key) + ")"
        definitions.append("    PRIMARY KEY (" + ", ".join([pk, *self.clustering_key]) + ")")
        order = ", ".join(f"{c} {direction}" for c, direction in zip(self.clustering_key, self.clustering_order))
        return (f"CREATE TABLE IF NOT EXISTS {prefix}{self.table_name} (\n"
                + ",\n".join(definitions)
                + f"\n) WITH CLUSTERING ORDER BY ({order});")


def build_prototypes(metrics: list[str], policy: dict[str, Any]) -> dict[str, CandidateSpec]:
    """Build only predefined, declared TS0/TS1/TS2/TS5/TS6 variants.

    A prototype becomes a candidate only if its family is active in at least
    one upstream query. Numbers in the policy are demonstration alternatives.
    """
    if len(metrics) != 10 or len(set(metrics)) != len(metrics):
        raise ValueError("Expected 10 unique CPU metric names from TSBS cpu-only schema")
    if not all(m.startswith("usage_") and m.replace("_", "").isalnum() for m in metrics):
        raise ValueError("Unsafe or unexpected TSBS metric identifier")
    for bucket in [*policy["raw_bucket_days"], *policy["wide_bucket_days"]]:
        if type(bucket) is not int or bucket < 1:
            raise ValueError("Bucket widths must be positive integer days")
    if len(set(policy["raw_bucket_days"])) != len(policy["raw_bucket_days"]):
        raise ValueError("Duplicate raw bucket-width candidates")
    if len(set(policy["wide_bucket_days"])) != len(policy["wide_bucket_days"]):
        raise ValueError("Duplicate wide bucket-width candidates")
    if policy["rollup_storage_bucket_days"] != 1:
        raise ValueError("Current TS5/TS6 implementation explicitly supports daily rollup storage only")
    if policy.get("select_winner_by_latency") or policy.get("automatic_ttl_selection") or policy.get("automatic_compaction_selection"):
        raise ValueError("Stage 4 must not pre-select performance winners or automatic physical modifiers")

    result: dict[str, CandidateSpec] = {}

    def add(family: str, suffix: str, table: str, cols: dict[str, str], pk: tuple[str, ...],
            ck: tuple[str, ...], ordering: tuple[str, ...], purpose: str,
            read: str, steps: tuple[str, ...], producer: str,
            modifiers: dict[str, Any] | None = None) -> None:
        cid = f"{family.lower()}_{suffix}"
        if cid in result:
            raise ValueError(f"Duplicate candidate {cid}")
        obj = CandidateSpec(cid, family, suffix, table, cols, pk, ck, ordering,
                            purpose, read, steps, producer, modifiers or {})
        obj.validate()
        result[cid] = obj

    add("TS0", "metric_raw", "ts0_cpu_metric_raw",
        {"hostname": "text", "metric_name": "text", "timestamp_ns": "bigint", "value": "double"},
        ("hostname", "metric_name"), ("timestamp_ns",), ("DESC",),
        "Unbucketed per-host/per-metric raw-series control; risk of growing partitions.",
        "SELECT timestamp_ns, value FROM ts0_cpu_metric_raw WHERE hostname = ? AND metric_name = ? AND timestamp_ns >= ? AND timestamp_ns < ?;",
        ("For multi-metric queries: fetch each metric and join by timestamp in client.",
         "For multiple/all hosts: enumerate host identities; Stage 5 must specify this source.",
         "For grouped MAX/AVG: aggregate retrieved points in client using exact time windows.",
         "For latest-per-host: fetch DESC LIMIT 1 per host and metric, then align fields."),
        "Stage 6: ingest each TSBS CPU observation into ten metric rows; not implemented.")

    for days in policy["raw_bucket_days"]:
        suffix = f"metric_bucket_{days}d"
        table = f"ts1_cpu_metric_{days}d"
        add("TS1", suffix, table,
            {"hostname": "text", "metric_name": "text", "bucket_day": "date",
             "timestamp_ns": "bigint", "value": "double"},
            ("hostname", "metric_name", "bucket_day"), ("timestamp_ns",), ("ASC",),
            f"Raw scalar readings in UTC buckets of {days} day(s).",
            f"SELECT timestamp_ns, value FROM {table} WHERE hostname = ? AND metric_name = ? AND bucket_day = ? AND timestamp_ns >= ? AND timestamp_ns < ?;",
            ("Compute UTC bucket_day at boundaries; issue one CQL read per bucket and metric.",
             "Apply non-key usage_user threshold and alignment/aggregation in client.",
             "Enumerate hosts for all-host queries; a complete QueryPlan is deferred."),
            "Stage 6: write every point to its UTC-aligned raw metric bucket; not implemented.",
            {"M1_bucket_days": days})

    for days in policy["wide_bucket_days"]:
        suffix = f"wide_bucket_{days}d"
        table = f"ts2_cpu_wide_{days}d"
        cols = {"hostname": "text", "bucket_day": "date", "timestamp_ns": "bigint"}
        cols.update({m: "double" for m in metrics})
        add("TS2", suffix, table, cols, ("hostname", "bucket_day"),
            ("timestamp_ns",), ("ASC",),
            f"Wide co-located 10-metric reading in UTC {days}-day bucket.",
            f"SELECT timestamp_ns, {', '.join(metrics)} FROM {table} WHERE hostname = ? AND bucket_day = ? AND timestamp_ns >= ? AND timestamp_ns < ?;",
            ("Apply usage_user > 90 in the client on returned rows, without ALLOW FILTERING.",
             "Enumerate affected UTC buckets; Stage 5 handles client plan completion."),
            "Stage 6: write an atomic logical CPU observation as one wide row; not implemented.",
            {"M1_bucket_days": days})

    for granularity, sortkey in [("minute", "minute_ns"), ("hour", "hour_ns")]:
        table = f"ts5_cpu_max_by_host_{granularity}"
        add("TS5", f"max_by_host_{granularity}", table,
            {"hostname": "text", "metric_name": "text", "bucket_day": "date",
             sortkey: "bigint", "max_value": "double"},
            ("hostname", "metric_name", "bucket_day"), (sortkey,), ("ASC",),
            f"Precomputed MAX per host/metric/{granularity} for TSBS time buckets.",
            f"SELECT {sortkey}, max_value FROM {table} WHERE hostname = ? AND metric_name = ? AND bucket_day = ? AND {sortkey} >= ? AND {sortkey} < ?;",
            ("For N hosts and M metrics issue supported partition reads and combine/sort results.",
             "Only complete buckets or explicitly correct boundary handling may serve a partial-time request."),
            "Stage 6: maintain maximum upon ingest including late/backfilled points; not implemented.",
            {"M1_storage_bucket_days": 1, "logical_aggregate_resolution": granularity})

    table = "ts5_cpu_sum_count_by_host_hour"
    add("TS5", "sum_count_by_host_hour", table,
        {"hostname": "text", "metric_name": "text", "bucket_day": "date",
         "hour_ns": "bigint", "value_sum": "double", "sample_count": "bigint"},
        ("hostname", "metric_name", "bucket_day"), ("hour_ns",), ("ASC",),
        "Exact AVG = SUM/COUNT per host/metric/hour; do not average averages.",
        f"SELECT hour_ns, value_sum, sample_count FROM {table} WHERE hostname = ? AND metric_name = ? AND bucket_day = ? AND hour_ns >= ? AND hour_ns < ?;",
        ("Compute AVG as SUM(value_sum)/SUM(sample_count) with correct NULL/zero policy.",
         "Enumerate all host identities before cross-host group-by; Stage 5 covers completeness."),
        "Stage 6: maintain non-duplicated SUM and COUNT for each metric/window; not implemented.",
        {"M1_storage_bucket_days": 1, "logical_aggregate_resolution": "hour"})

    table = "ts5_cpu_global_max_minute"
    add("TS5", "global_max_minute", table,
        {"metric_name": "text", "bucket_day": "date", "minute_ns": "bigint", "max_value": "double"},
        ("metric_name", "bucket_day"), ("minute_ns",), ("DESC",),
        "Global MAX usage_user per minute across all hosts for groupby-orderby-limit.",
        f"SELECT minute_ns, max_value FROM {table} WHERE metric_name = ? AND bucket_day = ? AND minute_ns < ? LIMIT 5;",
        ("Traverse historical daily buckets backward until five populated minutes are found.",
         "Ensure window's exclusive end bound and ordering; fill partial buckets correctly."),
        "Stage 6: maintain global MAX across hosts for each minute; not implemented.",
        {"M1_storage_bucket_days": 1, "logical_aggregate_resolution": "minute"})

    table = "ts6_cpu_by_hour_host"
    cols = {"bucket_day": "date", "hour_ns": "bigint", "hostname": "text"}
    for m in metrics:
        cols[f"sum_{m}"] = "double"
        cols[f"count_{m}"] = "bigint"
    add("TS6", "hour_host_dimension", table, cols,
        ("bucket_day", "hour_ns"), ("hostname",), ("ASC",),
        "Time+hostname grouped AVG across multiple series; one partition per UTC hour.",
        f"SELECT hostname, {', '.join(c for c in cols if c.startswith('sum_') or c.startswith('count_'))} FROM {table} WHERE bucket_day = ? AND hour_ns = ?;",
        ("Enumerate hourly windows, then emit rows by (hour, hostname).",
         "Compute each metric AVG from its sum/count; preserve missing-value handling.",
         "High host cardinality may create large partitions; feasibility guard deferred."),
        "Stage 6: write/refresh each grouped host/hour sum/count; not implemented.",
        {"M1_storage_bucket_days": 1, "logical_aggregate_resolution": "hour"})
    return result


# Determine TS5 physical specialization from workload kind (not query_id).
# TS5 activation continues to be decided strictly upstream by JSON rules.
TS5_SPECIALIZATIONS: dict[str, str] = {
    "single": "ts5_max_by_host_minute",
    "maxall": "ts5_max_by_host_hour",
    "double": "ts5_sum_count_by_host_hour",
    "toprollup": "ts5_global_max_minute",
}


def _activated(row: dict[str, str], prefix: str, allowed: frozenset[str]) -> set[str]:
    try:
        stated = json.loads(row[f"active_{prefix}"])
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Invalid active_{prefix} JSON for query {row.get('query_id')}") from exc
    if not isinstance(stated, list) or not set(stated) <= allowed:
        raise ValueError(f"Unexpected active_{prefix} labels for {row.get('query_id')}")
    expected = {x for x in allowed if row.get(x, "").lower() == "true"}
    if set(stated) != expected:
        raise ValueError(f"Activation booleans contradict active_{prefix} for {row['query_id']}")
    return set(stated)


def instantiate(root: Path, output: Path, policy_file: Path | None = None) -> dict[str, Any]:
    root = root.resolve()
    output = output.resolve()
    policy_path = policy_file.resolve() if policy_file else root / "config/tsbs_stage4_physical_policy.json"
    input_paths = {
        "activation_matrix": root / "outputs/tsbs_run/04_tsdb_activation_matrix.csv",
        "modifier_matrix": root / "outputs/tsbs_run/05_temporal_modifier_matrix.csv",
        "query_manifest": root / "inputs/workload/query_manifest.csv",
        "conceptual_schema": root / "inputs/schema/tsbs_cpu_only_schema.json",
        "temporal_catalog": root / "config/tsdb_family_catalog.json",
        "temporal_rules": root / "config/tsdb_activation_rules.json",
        "modifier_rules": root / "config/temporal_modifier_rules.json",
        "physical_policy": policy_path,
    }
    for name, path in input_paths.items():
        if not path.is_file():
            raise FileNotFoundError(f"Required Stage 3 source missing ({name}): {path}")
    manifest = read_csv(input_paths["query_manifest"])
    activation = read_csv(input_paths["activation_matrix"])
    modifiers = read_csv(input_paths["modifier_matrix"])
    schema = read_json(input_paths["conceptual_schema"])
    policy = read_json(policy_path)
    cat = read_json(input_paths["temporal_catalog"])
    tsrules = read_json(input_paths["temporal_rules"])
    modrules = read_json(input_paths["modifier_rules"])
    rule_families = {r["family"] for r in tsrules["rules"]}
    cat_families = {f["family"] for f in cat["families"]}
    if rule_families != cat_families or rule_families != TS_FAMILIES:
        raise ValueError("Original TS0-TS8 rules and catalog are inconsistent")
    if {r["family"] for r in modrules["rules"]} != TS_MODIFIERS:
        raise ValueError("Original M1-M5 modifier rules changed")
    if schema.get("dataset") != "TSBS cpu-only (DevOps)":
        raise ValueError("This implementation supports TSBS cpu-only only")
    metrics = schema["entities"]["CPUReading"]["temporal"]["metrics"]
    prototypes = build_prototypes(metrics, policy)

    def by_id(rows: list[dict[str, str]], label: str) -> dict[str, dict[str, str]]:
        result = {}
        for r in rows:
            qid = r["query_id"]
            if qid in result:
                raise ValueError(f"Duplicate {qid} in {label}")
            result[qid] = r
        return result

    manifest_by_id = by_id(manifest, "manifest")
    mods_by_id = by_id(modifiers, "modifier matrix")
    act_by_id = by_id(activation, "activation matrix")
    if not manifest_by_id or not(set(manifest_by_id) == set(mods_by_id) == set(act_by_id)):
        raise ValueError("Manifest, activation and modifier matrices must cover same query IDs")
    # Query IDs are used only for joining data. No family decision is keyed by Qnn.
    selected: dict[str, CandidateSpec] = {}
    links: list[dict[str, Any]] = []
    per_query: list[dict[str, Any]] = []
    unmapped: list[dict[str, str]] = []
    for qid, q in manifest_by_id.items():
        if (q["query_name"] != act_by_id[qid]["query_name"]
                or q["query_name"] != mods_by_id[qid]["query_name"]):
            raise ValueError(f"Inconsistent query name across input files for {qid}")
        active = _activated(act_by_id[qid], "families", TS_FAMILIES)
        active_mods = _activated(mods_by_id[qid], "modifiers", TS_MODIFIERS)
        kind = q["kind"]
        assignment: dict[str, list[str]] = {}
        for fam in sorted(active):
            choices: list[str] = []
            if fam == "TS0":
                choices = ["ts0_metric_raw"]
            elif fam == "TS1":
                choices = [f"ts1_metric_bucket_{n}d" for n in policy["raw_bucket_days"]]
            elif fam == "TS2":
                choices = [f"ts2_wide_bucket_{n}d" for n in policy["wide_bucket_days"]]
            elif fam == "TS5":
                if kind not in TS5_SPECIALIZATIONS:
                    raise ValueError(f"TS5 active for unrecognized query kind {kind}")
                choices = [TS5_SPECIALIZATIONS[kind]]
            elif fam == "TS6":
                if kind != "double":
                    raise ValueError(f"TS6 active outside non-time dimension group-by: {kind}")
                choices = ["ts6_hour_host_dimension"]
            else:
                # If new family is activated later, Stage 4 must report it,
                # not quietly claim all active families were materialized.
                unmapped.append({"query_id": qid, "family": fam,
                                 "reason": "NO_STAGE4_PHYSICAL_TEMPLATE"})
            for cid in choices:
                spec = prototypes[cid]
                assert spec.family == fam
                selected[cid] = spec
                links.append({"query_id": qid, "query_name": q["query_name"],
                              "query_kind": kind, "family": fam, "candidate_id": cid,
                              "active_modifiers": sorted(active_mods),
                              "coverage_status": "UNVERIFIED_STAGE5_QUERYPLAN_REQUIRED",
                              "write_status": "UNIMPLEMENTED_STAGE6_WRITEPLAN_REQUIRED"})
            assignment[fam] = choices
        per_query.append({"query_id": qid, "query_name": q["query_name"],
                          "kind": kind, "active_families": sorted(active),
                          "active_modifiers": sorted(active_mods),
                          "candidate_ids_by_family": assignment,
                          "stage4_mapping_complete": all(bool(assignment[f]) for f in active),
                          "query_plan_coverage_verified": False})

    if unmapped:
        # Fail closed if the input activation evolves: physical candidate gaps
        # cannot be silently reported as successful Stage 4 execution.
        raise ValueError(f"Activated family has no physical instantiator: {unmapped}")
    ddl_dir = output / "cql" / "schemas"
    if ddl_dir.exists():
        stale = {p.stem for p in ddl_dir.glob("*.cql")} - set(selected)
        if stale:
            raise ValueError(f"Stale CQL candidates in output directory: {sorted(stale)}; choose a fresh --output-dir")
    ddl_dir.mkdir(parents=True, exist_ok=True)
    for item in sorted(selected.values(), key=lambda x: x.candidate_id):
        (ddl_dir / f"{item.candidate_id}.cql").write_text(
            "-- Experimental Stage 4 CassandraCandidateSpec (schema only)\n"
            f"-- Family: {item.family}; variant: {item.variant}\n"
            f"-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED\n"
            + item.ddl() + "\n", encoding="utf-8")
    candidate_json = [asdict(c) for c in sorted(selected.values(), key=lambda x: x.candidate_id)]
    (output / "10_cassandra_candidate_specs.json").write_text(json.dumps(candidate_json, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (output / "11_query_candidate_map.json").write_text(json.dumps(per_query, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (output / "12_query_read_primitives.json").write_text(json.dumps(
        [{"candidate_id": c.candidate_id, "family": c.family,
          "query_template": c.read_cql_template, "application_requirements": list(c.client_steps),
          "status": "TEMPLATE_ONLY_NOT_EXECUTED"}
         for c in sorted(selected.values(), key=lambda x: x.candidate_id)], indent=2) + "\n", encoding="utf-8")
    with (output / "13_query_candidate_map.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["query_id", "query_name", "query_kind", "family", "candidate_id",
                                                 "active_modifiers", "coverage_status", "write_status"])
        writer.writeheader()
        for link in links:
            writer.writerow({**link, "active_modifiers": json.dumps(link["active_modifiers"])})
    report = {
        "stage": 4, "scope": "TSDB_ONLY_SCHEMA_INSTANTIATION",
        "candidate_count": len(selected), "query_count": len(per_query),
        "query_family_candidate_links": len(links),
        "candidate_counts_by_family": {fam: sum(c.family == fam for c in selected.values()) for fam in sorted(TS_FAMILIES)},
        "input_files_sha256": {name: file_hash(path) for name, path in input_paths.items()},
        "uses_cassandra_general_c0_c9": False, "uses_benchmark_latency": False,
        "activation_rules_modified": False, "source_stage3_reexecuted": False,
        "query_plan_coverage_verified": False, "write_plan_implemented": False,
        "benchmark_executed": False, "modifiers_are_not_implicitly_instantiated": True,
        "deferred": ["M2 TTL", "M3 compaction selection", "M4 salt/shard", "M5 backfill strategy",
                     "Stage 5 complete query/read plans", "Stage 6 load/write maintenance",
                     "Stage 8 workload consolidation/HBench", "Stage 9 benchmarks"],
        "external_baseline": "Original TSBS Cassandra series_double schema is NOT generated by this add-on. Compare in a separate deployment.",
        "cautions": [
            "TS0 unbucketed scalar design is NOT the TSBS baseline (which includes date in series_id).",
            "TS5 and TS6 are aggregate storage proposals; Stage 6 must maintain correct summaries.",
            "Q15 latest per host can be served through a multi-step TS0 raw plan; TS4 remains inactive.",
            "Q14 all-host high-CPU needs an enumerated host catalog and client-side filtering.",
            "Q16 global MAX minute query uses an additional global rollup and maintenance path; TS5 activation does not guarantee cheap maintenance.",
            "No production data has been inserted, no schema has been tested on a Cassandra server."
        ],
    }
    (output / "14_stage4_manifest.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report
