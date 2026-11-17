# SchemaLens / IoT Benchmark — Native Reference V3 Add-on

A small, additive, post-freeze extension for the specific V3 Cassandra experiment: an independent IoTDB-Table-shaped Cassandra reference, native Java query-object sample support, offline correctness validation, and a strict read-only measurement comparator. It does **not** change schema-family activation, Stage4–8, HBench or the original native dataset.

**Start here:** [`docs/GUIA_BASELINE_NATIVA_V3_PT.md`](docs/GUIA_BASELINE_NATIVA_V3_PT.md) (Portuguese).

## Contents

- `iot_pipeline/native_reference_v3.py`: independent reference materialization, Q1–Q12 physical read path, offline and opt-in live Cassandra benchmark.
- `iot_pipeline/compare_native_v3.py`: post-selection summary and provenance guards.
- `scripts/install_native_reference_v3.py`: verify hashes, add modules to isolated experiment, execute tests and offline checks.
- `tests/test_native_reference_v3.py`: 12 standalone tests for table layout, sparse values, fake Cassandra, set algebra, capture identity, frozen Stage8 integrity and comparison validation.

**Not an official IoT Benchmark Cassandra driver or benchmark score.** No native SQL/frequency replay. Tests passing on synthetic native-shaped fixtures do not prove equivalence with upstream IoTDB execution.

## Security

The only Cassandra DROP is in `python -m iot_pipeline.native_reference_v3 --live --confirm-reset RESET_SL_IOT_REF_NATIVE_V3`. The target is the dedicated `sl_iot_ref_native_v3` keyspace. No database work is performed by import, installation, or `--dry-run`.
