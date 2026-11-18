-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS5; variant: global_max_minute
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts5_cpu_global_max_minute (
    metric_name text,
    bucket_day date,
    minute_ns bigint,
    max_value double,
    PRIMARY KEY ((metric_name, bucket_day), minute_ns)
) WITH CLUSTERING ORDER BY (minute_ns DESC);
