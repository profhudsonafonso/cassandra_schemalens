-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS5; variant: sum_count_by_host_hour
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts5_cpu_sum_count_by_host_hour (
    hostname text,
    metric_name text,
    bucket_day date,
    hour_ns bigint,
    value_sum double,
    sample_count bigint,
    PRIMARY KEY ((hostname, metric_name, bucket_day), hour_ns)
) WITH CLUSTERING ORDER BY (hour_ns ASC);
