# SchemaLens + TSBS — TS-only Activation (Cassandra/Time-Series Research)

**Purpose:** apply exactly the existing SmartRetail *time-series* activation rules **TS0–TS8** and physical modifiers **M1–M5** to the independent TSBS `cpu-only` (DevOps) workload. **No generic Cassandra C0–C9 rules are loaded or executed.** This is the activation/trace stage, not generation of physical Cassandra tables, loading of data, or measurement of latency.

This package is a runnable adaptation of the uploaded `SchemaLens_TSDB_RunningExample_SmartRetail(2).zip`. The following are **byte-for-byte copies** of that original ZIP:

- `methodology/common/rule_engine.py` — original declarative rule evaluator, unchanged.
- `methodology/smart_retail/sql_evidence.py` — original controlled logical SQL extractor, unchanged.
- `methodology/smart_retail/evidence_builder.py` — original temporal feature builder, unchanged.
- `config/tsdb_activation_rules.json`, `config/tsdb_family_catalog.json` — original time-series rule set/catalogue, unchanged.
- `config/temporal_modifier_rules.json`, `config/temporal_policy.json` — original modifiers and demo policy, unchanged.

**New TSBS-specific code** lives only in `methodology/tsbs/`: `workload.py` (16 TSBS logical templates), `evidence_builder.py` (bridges the TSBS semantics to the original temporal feature definitions), and `run_tsbs.py` (TS-only pipeline, CSV/JSON audit outputs). We deliberately do not copy the C0–C9 configuration or execute the SmartRetail dual-family pipeline.

## 1. Run in VS Code or Ubuntu (Python 3.10+)

Open the directory `SchemaLens_TSBS_TSDB_Only` in VS Code, then use its integrated terminal:

```bash
python3 -m methodology.tsbs.run_tsbs
# or
./run_tsbs_tsdb_only.sh

# Re-run tests
python3 -m unittest discover -s tests -v
```

No external Python packages or running database are required. The command creates `inputs/schema/tsbs_cpu_only_schema.json`, the 16 controlled logical SQL files, `inputs/workload/query_manifest.csv`, and `outputs/tsbs_run/*`.

**Optional experimental assumptions (not claimed as TSBS defaults):**

```bash
./run_tsbs_tsdb_only.sh --scale 100 --interval-seconds 60 --days 3 \
  --retention-days 730 --allow-late-arrival \
  --output-dir outputs/tsbs_retention_backfill
```

These options **only alter evidence metadata**, so M2 and M5 can be assessed in a separately identified scenario. They do not change the rule predicates. The official TSBS generated workload is not being executed here. At least 32 hosts are required to cover `cpu-max-all-32-24`.

## 2. Output files (TS-only)

| File | Meaning |
|---|---|
| `01_input_inventory.csv` | Exact provenance paths, file sizes and SHA-256 hashes. |
| `02_combined_evidence.csv` | Evidence vectors derived **before activation**. |
| `04_tsdb_activation_matrix.csv` | One row per TSBS template; TS0–TS8 booleans and active set. |
| `05_temporal_modifier_matrix.csv` | One row per template; M1–M5 booleans. |
| `06_full_activation_trace.json` | Complete TS0–TS8 and M1–M5 `PASS/FAIL`, including condition details. |
| `07_query_explanations.csv` | Concise diagnostic evidence supporting each decision. |
| `08_family_catalog.csv` | Original temporal family names, definitions and purposes. |
| `09_run_manifest.json` | Scenario, activation counts, assumptions and limitations. |
| `inputs_snapshot/` | Per-run copies of all input SQL/JSON/CSV configs, for reproducibility. |

**There is intentionally no `03_cassandra_activation_matrix.csv`.** The numbering 04–09 was retained for compatibility with the familiar SmartRetail report names.

## 3. Fixed TSBS source and scenario

The `cpu-only` generator produces one observation **per host per log interval**, each containing the ten CPU metrics `usage_user`, `usage_system`, `usage_idle`, `usage_nice`, `usage_iowait`, `usage_irq`, `usage_softirq`, `usage_steal`, `usage_guest`, `usage_guest_nice`. Its ten static tags include `hostname`, `region`, `datacenter`, `rack`, `os`, `arch`, `team`, `service`, `service_version`, `service_environment`.

Relevant official TSBS source files (commit `8323e59c74027b108f4ad5ec5d3e498b0101a02e`, 2026-05-27):

- [TSBS query template registry](https://github.com/timescale/tsbs/blob/8323e59c74027b108f4ad5ec5d3e498b0101a02e/cmd/tsbs_generate_queries/main.go)
- [Cassandra query semantics](https://github.com/timescale/tsbs/blob/8323e59c74027b108f4ad5ec5d3e498b0101a02e/cmd/tsbs_generate_queries/databases/cassandra/devops.go)
- [CPU metric declarations](https://github.com/timescale/tsbs/blob/8323e59c74027b108f4ad5ec5d3e498b0101a02e/pkg/data/usecases/devops/cpu.go)
- [Host tags](https://github.com/timescale/tsbs/blob/8323e59c74027b108f4ad5ec5d3e498b0101a02e/pkg/data/usecases/devops/host.go)
- [CPU-only simulator](https://github.com/timescale/tsbs/blob/8323e59c74027b108f4ad5ec5d3e498b0101a02e/pkg/data/usecases/devops/cpu_only_generate_data.go)

Default **analytical** scenario: 100 hosts, one measurement every 60 seconds, 3 days (1440 observations/host/day; 144,000 across all hosts/day). The runtime **does not create or ingest TSBS data**. No query execution frequencies, retention horizon, or late-arrival policy are inferred from TSBS. `append_oriented=True` is an explicit interpretation of the default sequential time-series generation pattern, and M3 is therefore activated.

The 1,000,000-events-per-day hot-series threshold is copied unchanged from the SmartRetail **demo** policy, NOT calibrated from TSBS performance results. `estimated_events_per_day` means the estimated observations **per host's logical CPU series**, not the sum across all hosts and not the ten metric values per observation. Thus default M4 is not activated.

## 4. Conceptual schema (our adaptation, not an official TSBS EER)

- `Host` (hostname, tags) 1:N `CPUReading` (hostname, event_ts, ten CPU metrics).
- Temporal source: `CPUReading`; `series_identity=[hostname]`; `time_attribute=event_ts`.
- `hostname` is both the identity of a temporal series **and a non-time grouping dimension** for host-by-hour aggregates. This is necessary for TS6 to recognize TSBS `double-groupby` queries using the original rule; the rule itself is unchanged.
- The 1:N relation does **not** imply generic Cassandra containment rule C6 because **C families are not evaluated**.

The logical SQL in `inputs/workload/sql/` is *query-intent representation*, not executable CQL, and is not a claim that Cassandra natively supports `TIME_BUCKET` or `QUALIFY`. Randomized host IDs / concrete timestamps are intentionally represented by parameters because the activation is by query **type**, not by one random instance.

### Two fidelity issues addressed

**Half-open time windows:** TSBS uses `time >= start AND time < end`; the SmartRetail extractor recognizes `BETWEEN`. Our adapter normalizes the half-open expression to `BETWEEN` **only in a disposable string passed to the evidence extractor**, to derive `has_time_range=true`. Actual logical SQL retains its exclusive end bound. Do not execute the normalized expression as if it were semantically identical SQL.

**`lastpoint`:** TSBS requests the latest sample **per host**. The original TS4 predicate requires an `ORDER BY time DESC LIMIT 1` latest-value pattern. We represent the TSBS query as a conceptual windowed `ROW_NUMBER() OVER (PARTITION BY hostname ...) = 1`, and intentionally keep `latest_only=false` and **TS4 inactive**. This is a documented applicability gap to be considered in a future pre-registered methodological extension, not something patched after seeing benchmark results.

## 5. Sample results under the default scenario

| Upstream template(s) | Activated TS families | Modifiers |
|---|---|---|
| 6 `single-groupby-*` | TS0, TS5 | M1, M3 |
| 3 `cpu-max-all-*` | TS0, TS5 | M1, M3 |
| 3 `double-groupby-*` | TS0, TS5, TS6 | M1, M3 |
| `high-cpu-1` | TS0, TS1, TS2 | M1, M3 |
| `high-cpu-all` | TS0, TS1 | M1, M3 |
| `lastpoint` | TS0 | M3 |
| `groupby-orderby-limit` | TS0, TS5 | M1, M3 |

Aggregate counts: TS0=16, TS1=2, TS2=1, TS5=13, TS6=3; M1=15, M3=16. TS3/TS4/TS7/TS8 and M2/M4/M5 have no active instances with current default scenario evidence. This **does not** prove these families are ineffective; the standard TSBS workload or its provided metadata does not meet the original activation predicates.

## 6. Where this stops; next official research stages

**Not implemented:** automatic Cassandra PK/CK layout choices, CQL schemas, query/read plans, maintenance/write plans for duplicated or aggregate stores, workload-level consolidation, HBench selection, or TSBS read/ingest benchmarking. No observed latencies or historic baseline results are used to tune or alter activation rules.

Next: take the TS-only activated family sets as inputs to **Stage 4 CassandraCandidateSpec + PK/CK + CQL**. Preserve source-derived evidence and consider compatibility/coexistence across queries before using TSBS to benchmark complete viable deployments. Keeping a physically executable Cassandra baseline is necessary even for a query such as `lastpoint` whose specialized TS4 family is not activated in the unchanged rules.

## 7. Methodology provenance and limits

- **Reused unchanged:** generic SmartRetail rule engine, SQL extractor, time-series feature builder, TS0–TS8 catalogue/rules, M1–M5 rules and demo threshold policy.
- **Adapted:** DevOps/CPU conceptual schema, 16 logical query templates, half-open interval parser compatibility, experimental scenario inputs, trace/report driver.
- **No measurements:** outputs contain rule activations, not Cassandra performance, coverage proofs or complete physical schemas.
- **Future validation:** TS4 latest-per-group semantics, per-host versus per-metric time-series identity, aggregation plans for cross-host workloads, and high-cardinality filters may require independent extensions *before* testing a new held-out benchmark.
