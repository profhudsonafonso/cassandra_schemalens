# SchemaLens + IoT Benchmark + Cassandra — A1

**Status: first reproducible bridge, NOT a Cassandra benchmark.**

This add-on is a standalone Python 3.10+ educational/research preparation package. It retains the **unmodified original SchemaLens TS0–TS8/M1–M5 JSON activation rules and engine** from the user-provided SmartRetail/TSBS V2 source. C0–C9 are **not loaded**.

## Quick start — Visual Studio Code / terminal

```bash
unzip SchemaLens_IoT_Cassandra_A1.zip
cd SchemaLens_IoT_Cassandra_A1
./run_a1.sh
# or individually:
PYTHONPATH=. python3 -m unittest discover -s tests -v
python3 tools/run_a1.py
```

Runs without Cassandra, Maven, Docker, external PyPI dependencies or network, against the explicitly labelled **synthetic native-format fixture** (3 devices, 2 DOUBLE sensors, 35 non-null metric cells, repeated `Sensor` headers). The output is in `outputs/`.

## Use **actually generated** IoT Benchmark data

**Upstream source**: https://github.com/thulab/iot-benchmark (2026-09-15 release commit reference `46fc572`, pin an exact release for formal studies).

1. Install/compile the upstream IoT Benchmark (Java 17/Maven) following https://iotdb.apache.org/UserGuide/latest/Tools-System/Benchmark.html .
2. Prepare `conf/config.properties` in the upstream tool based on `inputs/iot_benchmark_pilot.properties`. **Check settings against that upstream version first.** This phase uses `BENCHMARK_WORK_MODE=generateDataMode` and `FILE_PATH=...`; no Cassandra adapter is claimed. The sample requests 3 devices, 2 DOUBLE sensors and `TIMESTAMP_PRECISION=ms`.
3. Run the benchmark generator to produce native `schema.txt`, `info.txt`, and `<device>/BigBatch_*.csv` files. NEVER point at a Cassandra keyspace for this generation step. NEVER use `IS_DELETE_DATA=true` against a non-dedicated database.
4. Import the generated directory:

```bash
PYTHONPATH=. python3 tools/run_a1.py \
  --native-data /path/to/generated/FILE_PATH \
  --max-rows 100000
```

**Strict restrictions of this A1 importer**: DOUBLE sensors only; all devices have the same sensor names; millisecond timestamps in the declared supported range; reject conflicting duplicates; skip null metric cells. Raise explicit errors rather than silently converting unsupported types. No cross-system equivalence claim is made until actual generated files and queries have been verified.

## Generated outputs

- `activation_ts_only.json`: actual TS-only activation calls and rule traces.
- `activation_table.csv`: query → active TS families and modifiers.
- `normalized_double_readings.csv`: canonical reading input to later Cassandra WritePlans.
- `query_instances_pilot.json`: **illustrative** Q1–Q12 parameter instances; not official `QUERY_SEED` replay.
- `reference_answers_pilot.json`: reference computation on this synthetic fixture; not official query verification.
- `cassandra_blueprints.cql` / `cassandra_ddl_blueprints.json`: five CQL DDL **blueprints** for TS0/TS1/TS2/TS4/TS5, no physical write and query implementation yet.
- `coverage_gates.json`: **NOT_VALIDATED** per activated query/candidate link, including conditional guards.
- `legacy_preflight.json`: inspection of original Stage4–9 ZIP contracts, when supplied.
- `manifest.json`: content hashes and explicit claims that have **not** been verified.

## Legacy compatibility inspection

To inspect **the six user-supplied ZIPs without modifying them**:

```bash
python3 tools/preflight_legacy.py --zips-dir /path/containing/stage4-9-zips \
  --out outputs/legacy_preflight.json
```

A1 does **not** merge TSBS Stage 4–9 into this project: Stage4 hardcodes exactly 10 `usage_*` CPU metrics; Stage8 only accepts TS0/TS1/TS2/TS5/TS6 and assumes a TSBS CPU shape; Stage9 requires TSBS `series_double,cpu,...` records, not IoT Benchmark device CSV. The stages remain reusable **architectural contracts**, but the IoT specific physical contracts must be implemented and tested first.

## Scientific firewall

`A(Qi)=alpha(Mi,G_TS)` precedes any benchmark. Neither measured latencies, benchmark winners, old TSBS execution measurements, nor Cassandra physical execution feedback may enter activation or HBench preselection. Source code enforces `benchmark_used_to_activate=false`, retains rule checksums and never imports C0–C9. No HBench was selected, no Cassandra was benchmarked, and the reference operations are not an equivalence proof against IoTDB-generated SQL.

See `docs/GUIA_A1_PT.md` for the detailed step-by-step study and expected next-phase tasks.
