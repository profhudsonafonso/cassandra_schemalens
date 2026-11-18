# Stage 9.1 Cassandra target verification fix

This patch adds a fail-fast identity check between the Cassandra Python driver
and the Docker container used for `nodetool` storage measurements.

## Quick start

```bash
python -m methodology.tpcx_iot.stage91.target_diagnostic \
  --hosts 127.0.0.1 --port 9042 \
  --container schemalens_tpcx_cassandra \
  --output outputs/stage91_target_diagnostic.json

python -m unittest discover -s tests -q
```

After receiving `MATCH`, rerun the same research benchmark with
`--nodetool-container` and `--require-storage`; it will automatically
write `69_stage91_target_preflight.json` before any keyspace reset.

See `docs/CORRECAO_IDENTIDADE_CASSANDRA_PT.md` for the full guide.
All analytical families, HBench configurations, and baseline remain frozen.
