# Complementary files only: SchemaLens TSBS Stage 4 (TS0–TS8 → CassandraCandidateSpec)

This package is **an additive patch for the previously delivered** `SchemaLens_TSBS_TSDB_Only` repository. No existing file is replaced. **Do not unzip it as a standalone project**: copy its contents into the root of your existing Stage 3 project, preserving the subdirectories.

## What you add (and ONLY what you add)

```text
config/tsbs_stage4_physical_policy.json                  NEW — explicit, non-optimized physical alternatives
methodology/tsbs/stage4/__init__.py                     NEW — Python package
methodology/tsbs/stage4/candidate_generator.py          NEW — Stage 4 CassandraCandidateSpec + PK/CK + CQL
methodology/tsbs/stage4/cli.py                          NEW — command-line entry point
run_tsbs_stage4.sh                                      NEW — run helper
tests/test_tsbs_stage4.py                               NEW — validation tests
docs/TSBS_STAGE4_GUIA_PT.md                             NEW — scientific explanations
README_STAGE4_ADDON.md                                  NEW — this file
```

## Run (Ubuntu / VS Code)

From the existing project root, after copying this add-on:

```bash
# Only if needed, regenerate the unmodified Stage 3 evidence/activation files:
python3 -m methodology.tsbs.run_tsbs

# Stage 4 operates on the files already generated in outputs/tsbs_run:
python3 -m methodology.tsbs.stage4.cli
# alternatively:
./run_tsbs_stage4.sh

python3 -m unittest discover -s tests -v
```

The script reads precomputed `outputs/tsbs_run/04_tsdb_activation_matrix.csv`, `05_temporal_modifier_matrix.csv`, `inputs/workload/query_manifest.csv`, and `inputs/schema/tsbs_cpu_only_schema.json`. It also validates the original JSON rule/catalogue families; **it does not evaluate, modify, or re-run them**.

## New generated outputs (not included in the add-on)

```text
outputs/tsbs_stage4/
  10_cassandra_candidate_specs.json  # actual PK/CK and column types
  11_query_candidate_map.json        # TS family → candidate IDs per query
  12_query_read_primitives.json      # CQL read primitives; NOT a complete QueryPlan
  13_query_candidate_map.csv         # human-readable mapping
  14_stage4_manifest.json            # source hashes, provenance, limitations
  cql/schemas/*.cql                  # one CQL CREATE TABLE per independent physical candidate
```

Current experiment produces 9 physical candidates and 37 query-to-candidate links from the existing 16 activated TSBS query templates. All numbers are independent of measured latency. **C0–C9 are absent.**

## Not included (later project phases)

The output is NOT a complete deployable workload schema. Read/query routing, verification of complete query coverage, reading all necessary host/time partitions, ingestion, incremental updates of MAX/AVG rollups, write amplification measurement, HBench consolidation, TSBS benchmark runs, and performance ranking are Stage 5+ tasks.

See `docs/TSBS_STAGE4_GUIA_PT.md` for a query-by-query explanation and the link to TSBS's own Cassandra baseline schema. Unlike TSBS's built-in `series_id` (which incorporates the date), TS0 here is an unbucketed logical raw-series control; do not mislabel it as a replica of the official baseline.
