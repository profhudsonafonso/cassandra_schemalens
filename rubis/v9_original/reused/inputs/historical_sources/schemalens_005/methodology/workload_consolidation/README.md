# IMDb Workload Consolidation Hbench Experiment

This folder contains the reproducibility package for the SchemaLens IMDb workload-level consolidation experiment.

The goal of this experiment is to evaluate complete MongoDB workload deployments selected by the SchemaLens physical consolidation step, instead of benchmarking isolated per-query configurations only. In the previous IMDb benchmark, each physical pattern such as `watchitem_g2`, `watchitem_g4`, or `series_g7` was measured separately. In this experiment, each benchmarked unit is a complete Hbench deployment, for example:

```text
H_rank_01 = series_g7 + watchitem_g0 + watchitem_g2 + watchitem_g4
```

The package supports two levels of reproducibility:

1. **Saved-result reproduction**: regenerate the final aggregate CSVs, comparison report, reduction table, and paper tables from the committed CSV outputs. This is the recommended lightweight reviewer path.
2. **Full benchmark reproduction**: regenerate Hbench configurations, materialize each complete MongoDB deployment, run latency benchmarks, collect query-plan evidence, validate integrity, and compare against the old benchmark.

---

## Folder structure

```text
methodology/workload_consolidation/
├── README.md
├── DATA_AND_ENVIRONMENT.md
├── generated_hbench/
│   ├── complete_workload_configurations_H.csv
│   ├── deployment_dedup_report.csv
│   └── hbench_topk.csv
├── inputs/
│   ├── analytical_matrix_final_df.csv
│   └── benchmark_aggregate_results_imdb_all_sfs.csv
├── results/
│   ├── sf0.25/
│   ├── sf0.5/
│   ├── sf1/
│   └── final/
└── scripts/
    ├── run_imdb_physical_consolidation_from_matrix_paper_v3.py
    ├── run_imdb_hbench_mongodb_deployments_qplan_resource_v6.py
    ├── validate_hbench_scale_integrity.py
    ├── generate_new_approach_imdb_all_sfs_aggregate.py
    ├── generate_final_comparison_report_imdb_hbench.py
    ├── measure_benchmarked_candidate_reduction.py
    ├── generate_performance_reduction_table_imdb_hbench.py
    └── merge_and_compare_final_hbench_imdb.py
```

---

## Main inputs

The experiment uses two committed input files:

```text
inputs/analytical_matrix_final_df.csv
inputs/benchmark_aggregate_results_imdb_all_sfs.csv
```

`analytical_matrix_final_df.csv` is the IMDb analytical matrix used to generate the workload-level physical candidate space.

`benchmark_aggregate_results_imdb_all_sfs.csv` is the old IMDb single-configuration benchmark aggregate file. It is used only for comparison and retrospective validation, not for selecting the new Hbench configurations.

---

## Main outputs

The final paper-facing outputs are under:

```text
results/final/
```

Important files:

```text
results/final/benchmark_aggregate_results_imdb_all_sfs_new_approach.csv
results/final/hbench_benchmark_aggregate_results_all_scales.csv
results/final/new_approach_all_sfs_aggregate_quality_report.md
results/final/comparison/final_comparison_report.md
results/final/comparison/hbench_vs_old_query_level_comparison_final.csv
results/final/comparison/hbench_vs_old_summary_hot_by_rank.csv
results/final/performance_reduction_table/overleaf_imdb_hbench_performance_reduction_table.tex
```

The final all-scale Hbench aggregate must contain:

```text
5 ranks × 3 scales × 10 queries × 2 phases = 300 aggregate rows
```

The raw benchmark output must contain:

```text
5 ranks × 3 scales × 10 queries × 2 phases × 10 repetitions = 3000 raw rows
```

---

## Lightweight reproduction from saved results

Use this path to regenerate the final reports and tables without rerunning MongoDB.

From the repository root:

```bash
cd methodology/workload_consolidation
python3 -m py_compile scripts/*.py
```

Generate the new all-scale aggregate file:

```bash
python3 scripts/generate_new_approach_imdb_all_sfs_aggregate.py \
  --run-dirs results/sf0.25 results/sf0.5 results/sf1 \
  --out-dir results/final/reproduced \
  --output-filename benchmark_aggregate_results_imdb_all_sfs_new_approach.csv \
  --top-k 5 \
  --write-compat-alias \
  --fail-on-incomplete
```

Generate the final comparison report against the old benchmark:

```bash
python3 scripts/generate_final_comparison_report_imdb_hbench.py \
  --new-aggregate-csv results/final/reproduced/benchmark_aggregate_results_imdb_all_sfs_new_approach.csv \
  --old-benchmark-csv inputs/benchmark_aggregate_results_imdb_all_sfs.csv \
  --out-dir results/final/reproduced/comparison \
  --fail-on-suspicious-zero
```

Measure benchmarked-candidate reduction:

```bash
python3 scripts/measure_benchmarked_candidate_reduction.py \
  --csv OLD=inputs/benchmark_aggregate_results_imdb_all_sfs.csv \
  --csv NEW=results/final/reproduced/benchmark_aggregate_results_imdb_all_sfs_new_approach.csv \
  --id-col OLD=config_name \
  --id-col NEW=rank \
  --baseline OLD \
  --out-dir results/final/reproduced/benchmark_reduction \
  --default-repetitions 10 \
  --manifest
```

Generate the combined performance/reduction table:

```bash
python3 scripts/generate_performance_reduction_table_imdb_hbench.py \
  --comparison-csv results/final/reproduced/comparison/hbench_vs_old_query_level_comparison_final.csv \
  --old-aggregate-csv inputs/benchmark_aggregate_results_imdb_all_sfs.csv \
  --new-aggregate-csv results/final/reproduced/benchmark_aggregate_results_imdb_all_sfs_new_approach.csv \
  --out-dir results/final/reproduced/performance_reduction_table
```

---

## Expected lightweight reproduction checks

After running the saved-result pipeline, check:

```text
results/final/reproduced/new_approach_all_sfs_aggregate_quality_report.md
results/final/reproduced/comparison/final_comparison_report.md
```

Expected conditions:

```text
scale factors: sf0.25, sf0.5, sf1
ranks: 1, 2, 3, 4, 5
queries: 10
phases: cold, hot
aggregate rows: 300
suspicious zero-returned read rows: 0
```

---

## Full benchmark reproduction overview

Full reproduction requires the IMDb scale-factor TSV folders and a running MongoDB instance. See `DATA_AND_ENVIRONMENT.md` before running this path.

### Step 1 — Generate Hbench top-k configurations

```bash
python3 scripts/run_imdb_physical_consolidation_from_matrix_paper_v3.py \
  --matrix-csv inputs/analytical_matrix_final_df.csv \
  --out-dir generated_hbench \
  --rho-max 3 \
  --top-k 5 \
  --verify-benchmark-csv inputs/benchmark_aggregate_results_imdb_all_sfs.csv
```

This creates or refreshes:

```text
generated_hbench/hbench_topk.csv
generated_hbench/complete_workload_configurations_H.csv
generated_hbench/deployment_dedup_report.csv
```

### Step 2 — Run one scale and one rank

The benchmark should be run rank by rank to control memory and disk usage.

Example for `sf0.25`, rank 1:

```bash
python3 scripts/run_imdb_hbench_mongodb_deployments_qplan_resource_v6.py \
  --hbench-csv generated_hbench/hbench_topk.csv \
  --results-dir results/sf0.25_rank01_rerun \
  --top-k 5 \
  --rank-list 1 \
  --scale-label sf0.25 \
  --run-phase cold hot \
  --repetitions 10 \
  --sample-size 20 \
  --batch-size 100000 \
  --embedding-batch-size 5000 \
  --batch-log-every 20 \
  --duckdb-memory-limit 16GB \
  --duckdb-threads 1 \
  --execution-db-prefix imdb_hbench_top5_sf025_rerun \
  --force-rebuild \
  --strict-full-scale-materialization \
  --log-resource-snapshots \
  --min-docker-free-gib 100 \
  --min-mem-available-gib 16 \
  --query-plan-after-benchmark \
  --explain-one-per-query
```

Change only the scale, rank, output folder, and database prefix for other runs:

```text
--scale-label sf0.25 | sf0.5 | sf1
--rank-list 1 | 2 | 3 | 4 | 5
```

### Step 3 — Validate each scale or rank

Example for one rank:

```bash
python3 scripts/validate_hbench_scale_integrity.py \
  --results-dir results/sf0.25_rank01_rerun \
  --scale-label sf0.25 \
  --top-k 1 \
  --phases cold hot \
  --repetitions 10
```

For full `sf1`, use strict count checks:

```bash
python3 scripts/validate_hbench_scale_integrity.py \
  --results-dir results/sf1 \
  --scale-label sf1 \
  --top-k 5 \
  --phases cold hot \
  --repetitions 10 \
  --strict-sf1-counts
```

A valid result must have no suspicious zero-returned read queries.

---

## Final reported result summary

The corrected final all-scale experiment benchmarked:

```text
5 complete Hbench deployments × 3 scale factors = 15 materialized deployments
```

The old single-configuration benchmark measured:

```text
9 single configurations × 3 scale factors = 27 materialized deployments
```

Thus, the new Hbench workload-level experiment reduced the number of materialized deployments from 27 to 15, a reduction of 44.4%.

The final hot-phase result reports the best measured workload-level deployment as:

```text
rank 2 = series_g8 + watchitem_g0 + watchitem_g2 + watchitem_g4
```

with mean normalized p95 against the old per-query best reported in the generated comparison files.

---

## Notes

Do not commit raw IMDb TSV files, MongoDB databases, Docker volumes, or failed smoke-test outputs.

The full benchmark is expensive. The recommended reviewer path is the saved-result reproduction pipeline above.
