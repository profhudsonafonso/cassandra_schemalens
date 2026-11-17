# Codex task — Implement and execute S2 (SchemaLens Rev3 Cassandra)

Work in the repository/workspace that produced SchemaLens Rev3 B and S1. Read `S2_PROTOCOL.md`, `s1_preregister.json`, `s1_cases.json`, `results/manifest.json`, and `results/AUDITORIA_S1_PT.md` first. Do not change, remove, or rewrite B/S1 keyspaces, results, configuration IDs or source hashes.

## Step A — Inspect before editing

1. Locate the Java TS benchmark generator, original B materialization, frozen candidate specs, Python driver/executor, S1 result code, Cassandra Docker/connection details, and correctness oracles.
2. Produce `s2_inventory.md`: actual file paths, git revision, known hashes, which of the density, pagination and partition metrics can be measured, and any discrepancies with S1. Resolve discrepancies before data generation.
3. Explicitly report whether the Java generator has a native scale/density parameter; if not, distinguish any deterministic synthetic-densification adapter.

## Step B — Implement with English code comments

Create scripts under a new `experiments/rev3_s2_partition_growth/` directory, preferably reusing the proven B/S1 code paths instead of rewriting query semantics. Components:

- `prepare_s2.py`: preflight; freeze checks; derive 4 densities; establish isolation and configuration mapping; write preregistration hashes.
- `materialize_s2.py`: create fresh keyspaces and load each density × variant; verify row counts, partition keys, DDL/indexes and full logical equivalence.
- `run_s2.py`: 5 warmups, 60 timed hot measurements, randomized balanced execution order, page size frozen from S1 as applicable, correct query plans and oracles, raw telemetry.
- `analyze_s2.py`: produce per-query/window/density p95 and D/U ratio, storage/partition statistics, plots, completeness/correctness audit, hashes and a concise interpretation.
- `README.md`: exact commands and prerequisites to reproduce in VS Code with no destructive defaults.

Do not run any destructive Cassandra operation on existing B/S1 keyspaces. A `--dry-run` must be the default for provisioning. Require explicit `--execute` and fresh S2 keyspace prefix for new DDL. Validate the target host and keyspaces before acting.

## Step C — Execute safely and audit

- Run static validation and `--dry-run` first; then use only fresh S2 keyspaces.
- Check correctness oracles before timing each case and after timing.
- Record both 60 measured samples and 5 warmups for each of 216 case–variant–density combinations.
- Persist raw latencies, order of operations, source/data/schema hashes, Cassandra version, hardware context, CQL counts, iterated rows, storage metrics and partition size distribution. If internal Cassandra reads cannot be measured, leave them unavailable; do not infer them from driver iteration counts.
- Stop and produce an explicit blocker report if any requirement cannot be met. Never fabricate or extrapolate measurements.

## Required outputs

Provide an `s2_results/` directory with `manifest.json`, `s2_raw_latencies.csv`, `s2_orders.csv`, `s2_query_p95.csv`, `s2_density_comparison.csv`, `s2_storage.csv`, `s2_partition_sizes.csv` (if measurable), `s2_correctness.csv`, `AUDITORIA_S2_PT.md`, and all exact code commands + commit hash. Compress all outputs into `rev3_S2_partition_growth_results.zip` for independent review.

**Scientific focus:** Under fixed history, at which observation density (if any) does daily bucketing outperform an unbucketed partition for a semantically equivalent query? Keep all inferences conditional on measurement uncertainty and experimental scope.
