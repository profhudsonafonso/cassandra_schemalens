# SchemaLens TSBS Stage 6 Add-on: WritePlans (TSDB families only)

**Additive files only**, for the existing repository containing SchemaLens
TSBS TSDB-Only and Stage 4 / Stage 5 add-ons. No existing module, activation
rule, schema or upstream output is overwritten. This is not a standalone repo.

## New files (merge their paths into the existing project ROOT)

```text
methodology/tsbs/stage6/__init__.py
methodology/tsbs/stage6/planner.py
methodology/tsbs/stage6/materializer.py
methodology/tsbs/stage6/tsbs_adapter.py
methodology/tsbs/stage6/cassandra_executor.py
methodology/tsbs/stage6/cli.py
config/tsbs_stage6_write_policy.json
run_tsbs_stage6.sh
tests/test_tsbs_stage6.py
examples/show_stage6_writeplans.py
docs/TSBS_STAGE6_GUIA_PT.md
README_STAGE6_ADDON.md
```

Extract into the current repository and **merge folder contents**, not an extra
`SchemaLens_TSBS_Stage6_ADDON/` subfolder. The ZIP intentionally contains **no
files from Stages 1–5**, no generated outputs and no DDL copies.

## Run from VS Code / Ubuntu

```bash
cd /path/to/SchemaLens_TSBS_TSDB_Only

# These outputs should exist from previous steps:
ls outputs/tsbs_stage4/10_cassandra_candidate_specs.json
ls outputs/tsbs_stage5/15_query_plans.json

# Generate WritePlan metadata (does NOT write to a database):
python3 -m methodology.tsbs.stage6.cli
# Or: ./run_tsbs_stage6.sh

# Demonstrate CPU inserts, corrected/backfilled values, and read equivalence:
python3 examples/show_stage6_writeplans.py

# Run ALL regression tests from previous stages + Stage 6:
python3 -m unittest discover -s tests -v
```

Only stdlib Python is needed for plan generation, mock materialization and
tests. The **optional** experimental Cassandra executor needs an externally
provided Cassandra-driver Session. It never runs by default.

## New output files (produced on command, excluded from ZIP)

```text
outputs/tsbs_stage6/
  18_write_plans.json          # nine table-specific INSERT/DELETE plans
  19_write_coverage_matrix.csv # links all 37 QueryPlans to WritePlans
  20_stage6_manifest.json      # source SHA256 + limits + status
```

Nine candidates: **4 raw** (TS0=1, TS1=2, TS2=1) and **5 derived** (TS5=4,
TS6=1). A dense 10-metric CPU point updates at most **63 physical rows** if
all 9 candidates are simultaneously materialized. This is an illustration,
NOT the cost of a future selected deployment; deploy only chosen candidates.

## Strict correctness and scientific scope

- TS0/TS1: upsert one row per CPU metric. TS2: one row with all ten metrics.
- TS5/TS6: rebuild only impacted summary groups from canonical observations;
  do not use naive Cassandra counters or MAX-only delta math.
- Duplicate/replayed observations do not increase SUM/COUNT.
- Corrected values, backfilled observations, and deletions correctly adjust
  MAX, SUM, COUNT in deterministic in-memory tests.
- Stage 5 synthetic read plans are checked against the Stage 6 physical rows.
- CPU-only pseudo-CSV parser consumes native TSBS Cassandra-formatted readings
  (all ten metrics required).
- No general Cassandra activation (C0–C9), HBench, or benchmark ranking is used.

**Limitations:** reference materializer keeps canonical readings and row data
in memory; no Cassandra cluster was tested, no real bulk loading executed, no
crash recovery or distributed atomic multi-table aggregate maintenance exists,
no production TTL/compaction/salting is selected. The opt-in Cassandra sink is
single-session sequential and NOT resilient to partial failures. Read
coverage retains Stage 5 guards for aligned windows and complete host rosters.

See [docs/TSBS_STAGE6_GUIA_PT.md](docs/TSBS_STAGE6_GUIA_PT.md) for a worked
Portuguese explanation of how each new file extends the methodology.
