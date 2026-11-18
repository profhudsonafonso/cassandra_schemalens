-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS5; variant: max_by_host_hour
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts5_cpu_max_by_host_hour (
    hostname text,
    metric_name text,
    bucket_day date,
    hour_ns bigint,
    max_value double,
    PRIMARY KEY ((hostname, metric_name, bucket_day), hour_ns)
) WITH CLUSTERING ORDER BY (hour_ns ASC);
