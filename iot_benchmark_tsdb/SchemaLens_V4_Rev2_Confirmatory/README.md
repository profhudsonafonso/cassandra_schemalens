# SchemaLens V4 Rev2 — Independent confirmatory study

English source code and explanatory comments. For step-by-step instructions, experimental methodology, protection of the frozen V3/V4 Rev1 benchmarks, and caveats, see **README_PT.md**.

Workflow: `run_tests.sh` -> `run_rev2.sh ORIGINAL_V3` (exploratory software regression) -> `python -m v4_rev2.prepare_holdout` (new pinned Java generation) -> `run_rev2.sh NEW_EXPERIMENT --confirmatory --reference-v3 OLD` (immutable Stage7/8 freeze) -> `run_confirmatory.sh NEW_EXPERIMENT` (live Cassandra, explicit reset opt-in).

The original V3 Stage4–9 source files and frozen selection are never modified. Confirmation requires new data and query-object SHA-256 values; no measured performance is used before freezing the new selection. This is **not** an official IoTDB SQL/frequency benchmark run.
