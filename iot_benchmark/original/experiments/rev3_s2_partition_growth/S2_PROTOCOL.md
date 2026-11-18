# SchemaLens Rev3 — S2 Partition-Density Growth Protocol

Status: **PROPOSED / NOT EXECUTED**. This is an experimental preregistration draft based on the S1 archive; freeze and timestamp/hash it before new S2 measurements.

## Goal

Determine how increasing observation density within a **fixed ~20.82639-day history** affects the three frozen physical configurations. Test whether TS2-D (daily buckets) crosses over TS2-U (unbucketed) as the data per partition grow, while accounting for the extra CQL calls introduced by daily buckets.

## Frozen provenance from S1

- Source: `rev3_S1_window_sweep_results(2).zip` and `s1_preregister.json`.
- Configuration identifiers: `BASELINE_Q8_OPTIMIZED`, `R3H_38939db96444` (TS2-U), `R3H_d2ea1974de53` (TS2-D).
- Existing S1: 9 devices, 2 sensors, 54,000 source cells, window anchor `1704067200000`, first observation `1704067800000`, last observation `1705867200000` milliseconds since epoch UTC.
- Original Rev3 B freeze SHA-256: `8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231`.
- Source data SHA-256 (S1): `047c269a42beea21333d7dbecdbde798862346015a35a5b1ced232b05a8f2c2a`.
- Query input SHA-256 (S1): `e77aa9ab1a284fdad41d00055435ea2e8f50062bba5518aaf149d3f70fd77de5`.
- S1 query cases SHA-256: `31fd36c631d5ef2430087f57bb1fdfd000dbf956e415c64db5577d8c6c32d3e7`.
- Original B workspace recorded by S1: `/home/hudson/Documents/schemalens_cassandra/schemalens_rev3_new_java_B_20261105_193618`. Check existence; do not assume the path is accessible elsewhere.
- S1 host/port were `127.0.0.1:9043`. These are provenance, **not** guaranteed S2 connection settings.

## Experimental factors

- Independent variable: observation-density multiplier `m ∈ {1,2,4,8}`.
- Fixed: same 9 logical devices, 2 sensors, same observation time span, frozen schema definitions, partition-key logic, indexes, query semantics, driver fetch/page size 16 where applicable.
- Expected logical source cells: 54,000; 108,000; 216,000; 432,000. This is an **experimental target**, not a count already loaded.
- For each density and physical configuration, use an independently named database/keyspace to prevent overwriting B/S1. Names must be verified against Cassandra schema before any DDL.
- Queries: Q2, Q7, Q9, Q12 with windows of 1,7,14,21 days; Q5 and Q8 as controls. Use S1 case definitions, preserving predicates, ordering, aggregation, set-operation semantics, and anchors.
- Measurement plan: 5 warmups and 60 measured runs per query-case/variant/density; randomized balanced order with recorded seed and full order log. S1's seed 20261127 is provenance; choose and preregister a distinct S2 seed.
- Cases: 18 query-case IDs (4 queries × 4 windows + 2 controls), 3 variants, 4 densities = 216 combinations; **12,960 measured observations**, plus **1,080 warmups**, assuming all levels and variants successfully materialize.

## Density scaling: mandatory design constraints

1. Inspect the original Java generator and B/S1 data lineage. Prefer regenerating data from the original Java generator with a documented density parameter where genuinely supported. Do not claim official generator support without verifying code.
2. If no native density parameter exists, implement a transparent deterministic derivation as a *synthetic stress workload*, keeping timestamps within the frozen interval and distinct primary keys; explicitly document that data are derived rather than independent official Java output. Preserve value distributions where feasible, report deviations, and use fixed deterministic seed.
3. Preserve query equivalence. Do **not** silently duplicate rows with identical primary keys or generate new entities that a query cannot address. Define a single logical oracle over each generated density; compare result contents (or exact normalized result hashes) across all variants.
4. Clearly distinguish **source rows/cells**, Cassandra live partition sizes, SSTable bytes, driver-iterated rows, and `Session.execute` calls. The latter two are client instrumentation, not Cassandra's internal physical reads.
5. Freeze query plan/code paths and all schema definitions *before* S2 measurements; validate expected row counts and output equivalence.

## Run controls

- Record machine, Cassandra version, Docker configuration, CPU/RAM, concurrent processes, Cassandra state, compaction status, page size, replication, consistency level, compression, partitioner, and table DDL.
- For each density × variant: provision unique keyspace, load dataset, validate exact logical counts and data coverage, stabilize compactions/flushes under a fixed documented rule, capture storage and partition-level evidence, then measure under balanced randomized ordering.
- Do not benchmark one variant under a different flush/compaction protocol. Cold and hot phases must be separated and labeled; **primary S2 metric is hot-run p95**.
- Write operations: if assessing ingestion/write amplification, collect separately under resettable state; do not mix load timing with query latency.
- Capture raw per-run durations, query+window+variant+density, correctness status, parameter hash, seed/order index, executed CQL statement count, iterated rows, error status, and environment manifest.

## Primary and secondary analysis

- Primary: per-query/window/density hot p95 TS2-U vs TS2-D with ratio `p95_D / p95_U`; baseline separately.
- Secondary: median, mean, p99, dispersion, bootstrap confidence intervals as appropriate (note per-query-run dependence and execution ordering), call counts, iterated rows, partition-size distribution (median/p95/max), SSTable/storage bytes, ingestion time, write amplification proxies.
- A crossover means a density where the observed `p95_D / p95_U` changes from >1 to <1 *under otherwise equivalent runs*, not a one-off noisy difference. Require repeatability and uncertainty reporting before describing a reliable inversion.
- Compare against S1 only qualitatively unless environment and postflush context are verified equivalent. S1 is post-A/B exploratory, not independent holdout.

## Acceptance checklist

- [ ] Frozen schema hashes and keyspace isolation documented
- [ ] Java source / derivative generation explicitly classified
- [ ] Row count and logical oracle PASS for all 4 densities and 3 variants
- [ ] Partition size and storage metrics captured or marked unavailable
- [ ] All 216 combinations have 60 measured hot runs and 5 warmups; missing observations are explicitly accounted for
- [ ] 12,960 raw latency rows and 1,080 warmup rows, if complete
- [ ] Balanced execution order persisted, query results equal across variants
- [ ] Result CSV hashes and environment manifest recorded
- [ ] Interpretation distinguishes client-side calls from physical reads
- [ ] No S1/B keyspace overwritten or schema accidentally modified

## Stop conditions

Stop and report rather than proceeding if original B/S1 physical plans cannot be recovered, schema hashes differ, output oracle fails, density generator changes query semantics, or unique keyspace isolation cannot be verified. Never silently substitute a new chosen schema.
