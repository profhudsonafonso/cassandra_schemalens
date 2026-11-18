# SchemaLens Cassandra Rev3 — Independent Java Confirmatory Runs

See `README_PT.md`. This add-on runs two separate Java-generated datasets (A and B), verifies Rev2/Rev3 frozen artifacts, and benchmarks the selected physical plans and fair reference baselines in fresh create-only Cassandra keyspaces. It does **not** replay official IoT Benchmark SQL or operation frequencies and does **not** modify earlier experiments.

Quick start from the root of the user's Cassandra project:

```bash
unzip -o SchemaLens_Rev3_Independent_Confirmatory.zip
PACK="$(realpath SchemaLens_Rev3_Independent_Confirmatory)"
bash "$PACK/run_tests.sh"
bash "$PACK/run_campaign.sh" A generate
bash "$PACK/run_campaign.sh" A freeze
bash "$PACK/run_campaign.sh" A preflight
bash "$PACK/run_campaign.sh" A benchmark
bash "$PACK/run_campaign.sh" A pack
```

Repeat with `B` after completing and preserving A. Only run against a disposable Cassandra test instance. The program creates new keyspaces and does not DROP existing ones.
