# SchemaLens + TPCx-IoT — Cassandra — Stages 1–9 (research pilot)

This package preserves the complete original Stages 1–8 and adds **Stage9**: a reproducible driver for validating the five previously selected workload-level HBench configurations against a real Apache Cassandra instance, followed by a concurrent read/ingest research microbenchmark.

**Completed in this artifact:** deterministic offline dry-run, selection integrity checks, local SQLite outbox/replay, CQL deployment/driver, correctness oracle, concurrent benchmark runner, Docker Compose for a one-node local pilot, 122 passing unit/regression tests. **Not executed in this artifact:** live Cassandra (no Docker/driver available in the build environment), official licensed TPCx-IoT driver, compliant TPC benchmark, real latency/throughput/storage results.

The frozen workload is the **sensor-name-only/raw_read hypothesis** introduced at Stage1–3; the official kit binding is not verified. It must not be represented as compliant TPCx-IoT 2.1.2. The original specification is available at https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPCX-IOT_v2.1.2.pdf.

## Minimal run (dry-run/offline tests)

```bash
python3 -m methodology.tpcx_iot.stage9.cli dry-run
python3 -m unittest discover -s tests -v
# or ./run_stage9.sh
```

## Actual local Apache Cassandra run

```bash
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements-stage9.txt
docker compose -f docker-compose.stage9.yml up -d
# Wait until container healthcheck is healthy
python -m methodology.tpcx_iot.stage9.cli smoke --reset
python -m methodology.tpcx_iot.stage9.cli benchmark --reset --bench-steps 500 --read-threads 2
```

**Caution:** `--reset` deletes research keyspaces named `sl_tpcx_h_*` and their local experiment outbox ledgers. Docker deployment has **RF=1**; not compliant with minimum two-way replication in the original TPC spec. `repair` mode can replay **persisted** unacknowledged mutations after a partial failure without dropping existing Cassandra state.

## Files

- `docs/ETAPA9_CASSANDRA_GUIA_PT.md` — detailed Portuguese walkthrough, methodology, scientific limits, commands and explanations.
- `methodology/tpcx_iot/stage9/` — execution implementation, English docstrings.
- `docker-compose.stage9.yml` — local one-node Cassandra pilot.
- `requirements-stage9.txt` — Python Cassandra driver.
- `outputs/tpcx_iot_stage9/60_stage9_execution_manifest.json` — computed dry-run plan with locked Stage8 source hashes.
- `tests/test_tpcx_stage9.py` — 20 new tests. Total: 122 tests.

All earlier Stages 1–8 and their outputs are retained for auditable re-execution; Stage9 never alters template activations or HBench.
