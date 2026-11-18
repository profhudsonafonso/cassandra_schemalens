# SchemaLens + TSBS — Stage 9 incremental add-on

## Scope

This ZIP contains **only new Stage 9 paths**. Place its contents at the root of the existing **Stages 3–8** project. It does **not** replace the original rule engine, TS0–TS8/M1–M5 catalogs, generated physical candidates, QueryPlans, WritePlans, analytic cost proxies, or the frozen HBench output.

**Purpose:** bounded post-selection experimental Cassandra assessment for the seven **already-selected** Stage 8 deployments, using the same cpu-only TSBS data, 16 logical query templates, correctness oracle, and a TSBS-structured scalar-series baseline. No benchmark result is allowed to reconfigure the earlier methodology or change the candidate ranks.

**Not claimed:** successful live Cassandra execution, a scalable distributed loader, fully fault-tolerant TS5/TS6 maintenance, byte-identical TSBS Go-generated queries, proof of near-best optimality, official TSBS benchmark scores, or a TimescaleDB-vs-Cassandra evaluation. The official Go TSBS Cassandra query executor is NOT invoked by this Python add-on. The baseline mimics the original TSBS Cassandra `series_double(series_id,timestamp_ns,value)` physical key and ingest serialization, excluding deprecated `COMPACT STORAGE`.

## What was added

```text
README_STAGE9_ADDON.md
requirements_stage9_optional.txt
run_tsbs_stage9.sh
docker-compose.stage9.yml          # optional local Cassandra 4.1; not tested here
config/tsbs_stage9_benchmark_policy.json
methodology/tsbs/stage9/
    __init__.py
    contracts.py
    dataset.py
    instances.py
    offline.py
    driver.py
    benchmark.py
    cli.py
tests/test_tsbs_stage9.py
docs/TSBS_STAGE9_GUIA_PT.md
```

No files from an earlier stage are copied or overwritten.

## 1. Validate safely WITHOUT Cassandra

In the VS Code terminal at the combined project's root:

```bash
python3 -m methodology.tsbs.stage9.cli \
  --make-fixture inputs/stage9_dev_fixture.csv \
  --fixture-hosts 40 --fixture-hours 36 --fixture-interval-seconds 3600

# Uses the real frozen Stage 8 selection but an explicitly SYNTHETIC fixture.
python3 -m methodology.tsbs.stage9.cli \
  --data inputs/stage9_dev_fixture.csv --all --dry-run \
  --alignment-cohort natural --output outputs/tsbs_stage9_natural_offline

python3 -m methodology.tsbs.stage9.cli \
  --data inputs/stage9_dev_fixture.csv --all --dry-run \
  --alignment-cohort aligned --output outputs/tsbs_stage9_aligned_offline

python3 -m unittest discover -s tests -v
```

The synthetic fixture is **not official TSBS-generated data**. It only tests the protocol and data format. The script enforces 40 or more hosts for the `cpu-max-all-32-24` template and at least a 24-hour query window.

In Stage 9 **dry-run**, query correctness is checked using an indexed adapter over the earlier Stage 6 **in-memory** materializer. No SQL/CQL is sent to Cassandra; no latency or throughput measurements may be reported. The run creates files named `33`–`36` under its chosen output directory.

## 2. Generate REAL TSBS input

Install/build TSBS using the **pinned commit recorded by the existing project**, and follow its current `README.md` and `docs/cassandra.md`. Generate **cpu-only / Cassandra serialized data** with at least 32 hosts, at least 24 hours, and a **sampling interval that matches** `config/tsbs_stage9_benchmark_policy.json` (`sample_interval_seconds`, default 3600). Supply the native text output (or `.gz`) directly to `--data`. The adapter checks all 10 complete CPU metrics, original tag consistency, timestamps and series ids.

**Important:** the original TSBS default data scales can be too large for Stage 6's in-memory reference materializer. `max_observations_in_memory` defaults to 5000, failing with an error rather than truncating. This Stage 9 implementation is a **limited protocol-validation experiment**, not the final large-scale benchmark harness.

For the 10-second TSBS workload, set `sample_interval_seconds` to `10` in this Stage 9 policy BEFORE running benchmarks; choose a time range/scale whose number of complete observations fits the cap.

## 3. Run against a disposable Cassandra server

1. Ensure a disposable Cassandra server is running and accessible on `127.0.0.1:9042` (or specify host/port). Optionally run `docker compose -f docker-compose.stage9.yml up -d` and wait for the container healthcheck to succeed. The Compose template itself was **not executed or validated** in the authoring environment.
2. Install the optional client in your venv: `python3 -m pip install -r requirements_stage9_optional.txt`.
3. Identify a **preselected**, frozen Stage 8 deployment in `outputs/tsbs_stage8/30_hbench_summary.csv`.
4. Run a small **single-deployment** experiment, explicitly approving the reset of only that experiment's isolated keyspace:

```bash
python3 -m methodology.tsbs.stage9.cli \
  --data inputs/stage9_dev_fixture.csv \
  --live --host 127.0.0.1 --port 9042 \
  --deployment H_815d3cd1f0cb \
  --confirm-reset-keyspace sl_tsbs_h815d3cd1f0cb \
  --output outputs/tsbs_stage9_live_conservative_run1
```

**Destructive action:** `--live` drops and recreates the named `sl_tsbs_*` test keyspace. The command REQUIRES matching explicit confirmation before connecting. Never point it at a production Cassandra service. Each deployment has its own named test keyspace.

Once verified, you can run all seven HBench selections; this drops/recreates **all seven corresponding isolated keyspaces**:

```bash
python3 -m methodology.tsbs.stage9.cli \
  --data inputs/stage9_dev_fixture.csv \
  --live --all --confirm-reset-all RESET_ALL_SL_TSBS \
  --alignment-cohort aligned \
  --output outputs/tsbs_stage9_live_aligned_run1
```

Use a **new --output** path for each repetition; live mode refuses to overwrite an existing result manifest unless `--overwrite-results` is supplied.

## 4. Result artifacts (NEW only)

```text
outputs/tsbs_stage9*/
    33_stage9_workload_instances.json
    34_stage9_deployment_runs.json
    35_stage9_query_latencies.csv
    36_stage9_manifest.json
```

`35_stage9_query_latencies.csv` is empty apart from headers in dry-run mode. In live mode it records per-instance end-to-end client latency mean/p50/p95/p99 and the adapted TSBS baseline mean, **only after a correctness check**. An invalid plan stops further measurements for that deployment; manifest status says `INVALID_NO_PERFORMANCE_CLAIM`.

The output manifest contains source and upstream SHA-256, frozen query seed, alignment cohort, host/data cardinality, deployment IDs, validation status and unverified claims. The Cassandra `storage_bytes` field is **null** pending independent server measurement.

## Scientific protocol and constraints

* Always compare identical logical parameters on the same cluster, consistency, cache and load protocol. For concurrent tests, extend the runner and label concurrency explicitly; this release has **one worker**.
* **Natural cohort** = random second-resolution boundaries (not a byte replay of TSBS Go RNG). Many precomputed rollups cannot exactly answer such windows; Stage 5 guards force TS0 fallback.
* **Aligned cohort** = deliberate controlled follow-up case with minute/hour boundaries. Use *separate results* and never pass them off as naturally sampled TSBS workload.
* The baseline key is consistent with TSBS's `pkg/targets/cassandra/serializer.go` and `creator.go`, **without** legacy `COMPACT STORAGE`, and queries are executed through the **same Stage 5 logical client**. This ensures within-project comparison; for a literal native-TSBS score you still need the unmodified Go runner separately.
* Initial load applies the Stage 6 write actions from **complete in-memory** observations. It does not guarantee crash recovery, stream replays, concurrent backfill correctness, or high-throughput ingest. Observe the initial load duration but do not infer continuous update throughput.
* The Stage 8 selection remains frozen. To estimate Top-k preservation/regret, independently benchmark a wider VALID reference catalog after the HBench result is fixed and only compare retrospectively.
* No real Cassandra run was possible in the implementation environment. **Run live commands locally, inspect the validation results, and only then make performance claims.**

See `docs/TSBS_STAGE9_GUIA_PT.md` for explanations in Portuguese.
