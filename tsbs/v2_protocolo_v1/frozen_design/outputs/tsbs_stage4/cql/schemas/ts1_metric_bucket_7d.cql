-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS1; variant: metric_bucket_7d
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts1_cpu_metric_7d (
    hostname text,
    metric_name text,
    bucket_day date,
    timestamp_ns bigint,
    value double,
    PRIMARY KEY ((hostname, metric_name, bucket_day), timestamp_ns)
) WITH CLUSTERING ORDER BY (timestamp_ns ASC);
