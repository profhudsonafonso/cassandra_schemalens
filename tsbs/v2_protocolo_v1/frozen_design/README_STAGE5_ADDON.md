# TSBS Stage 5 Add-on: QueryPlan + Conditional Read Coverage (TS0–TS8 only)

**ONLY additional files**, for an existing checkout containing:
1. SchemaLens TSBS TSDB-Only (Stage 3); and
2. SchemaLens TSBS Stage 4 Add-on (CassandraCandidateSpec).

This package does **not** overwrite any source, activation rule, configuration,
Cassandra schema, or generated Stage 3/Stage 4 artifact.

## New files to copy to the EXISTING project root

```text
methodology/tsbs/stage5/__init__.py
methodology/tsbs/stage5/planner.py
methodology/tsbs/stage5/runtime.py
methodology/tsbs/stage5/cli.py
run_tsbs_stage5.sh
tests/test_tsbs_stage5.py
docs/TSBS_STAGE5_GUIA_PT.md
examples/show_stage5_queryplans.py
README_STAGE5_ADDON.md
```

Do NOT copy the enclosing `SchemaLens_TSBS_Stage5_ADDON` folder; merge its
contents at your existing project root. It intentionally includes no precomputed
outputs and no copies of old files.

## Run in Ubuntu / VS Code

```bash
cd /path/to/SchemaLens_TSBS_TSDB_Only

# Only if not already done: create stage-4 outputs with your existing code.
python3 -m methodology.tsbs.stage4.cli

# This add-on reads Stage 4; it does not modify or rerun its activation rules.
python3 -m methodology.tsbs.stage5.cli
# Equivalent:
./run_tsbs_stage5.sh

python3 -m unittest discover -s tests -v
python3 examples/show_stage5_queryplans.py
```

No non-standard Python packages are required by the Stage 5 add-on.

## New generated outputs (not shipped in ZIP)

```text
outputs/tsbs_stage5/
  15_query_plans.json       # bound-read recipe and explicit client operations
  16_coverage_matrix.csv    # one row per query/candidate
  17_stage5_manifest.json   # counts, source SHA256 hashes, limitations
```

Observed on the provided Stage 4 artifacts: **16 queries, 9 schema candidates,
37 query/candidate plans**. Of these, **16 depend on fully aligned temporal
windows** (13 TS5 plus 3 TS6). The other 21 require other runtime conditions,
including actual data materialization, exact host identifiers, and/or dense
CPU metric observations. None has been run against live Cassandra yet.

The Stage 5 QueryPlan can:
- enumerate CQL requests with bound parameters and day/week/hour fan-out;
- reject a missing/incomplete host-roster assertion;
- reject partial aggregate windows (rather than silently produce wrong answers);
- assemble scalar metrics / wide rows and compute MAX, AVG, threshold search,
  latest per host, and last five time buckets on synthetic physical rows;
- validate simplified CQL partition-key constraints, stage provenance, and
  the logical source SQL contract.

**Important limitations:** The `execute_mock_rows` method processes *supplied*
rows; it is an in-memory semantic unit-test facility, **not** a Cassandra
query execution result. Stage 6 still needs loaders and correct incremental
MAX/SUM/COUNT maintenance, including backfill. There is no claim of live
coverage, full deployable schema, measured latency, correct Cassandra server
behavior, or HBench success yet. Do not use simulated results as benchmark
measurements.

Details: [docs/TSBS_STAGE5_GUIA_PT.md](docs/TSBS_STAGE5_GUIA_PT.md).
