# SchemaLens Cassandra Rev3 — Plan-Aware Selection

An additive Stage5/7/8 prebenchmark freeze plus a read-only Stage9 semantic probe.

Read [README_PT.md](README_PT.md) for the complete Portuguese methodology and commands.

- Immutable Rev2 and V3 source/frozen artifacts are prerequisites.
- No observed latencies or benchmark CSVs are inputs to selection.
- Q8 TS0 FULL_SCAN vs. DESC_LIMIT1 are explicit executable alternatives.
- Stage8 selects canonical sources and per-query physical plans together.
- The read-only Stage9 probe requires an existing matching Rev2 materialization and never writes Cassandra data.
- This is *exploratory* post-E5 methodology, not independent performance validation.
