# SchemaLens TPCx-IoT + Cassandra — Stage 9.1

This is an **additive extension** of Stages 1–9; previous frozen inputs remain unchanged.

It compares five frozen HBench deployments with a **TPCx-IoT logical-record flat-table baseline ported to Cassandra**, and offers a separate *after-selection* verification mode over all nine complete Stage8 deployments. The Cassandra port **is not an official TPCx-IoT physical schema**. The resulting metrics are research-only and never use official IoTps terminology.

Run `python3 -m methodology.tpcx_iot.stage91.cli dry-run` and read [`docs/ETAPA9_1_COMPARACAO_BASELINE_PT.md`](docs/ETAPA9_1_COMPARACAO_BASELINE_PT.md).

Run all tests with `python3 -m unittest discover -s tests -v` (142 expected in this release).
