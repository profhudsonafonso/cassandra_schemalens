-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS0; variant: metric_raw
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts0_cpu_metric_raw (
    hostname text,
    metric_name text,
    timestamp_ns bigint,
    value double,
    PRIMARY KEY ((hostname, metric_name), timestamp_ns)
) WITH CLUSTERING ORDER BY (timestamp_ns DESC);
