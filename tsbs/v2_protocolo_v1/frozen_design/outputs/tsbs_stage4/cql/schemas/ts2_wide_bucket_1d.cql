-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS2; variant: wide_bucket_1d
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts2_cpu_wide_1d (
    hostname text,
    bucket_day date,
    timestamp_ns bigint,
    usage_user double,
    usage_system double,
    usage_idle double,
    usage_nice double,
    usage_iowait double,
    usage_irq double,
    usage_softirq double,
    usage_steal double,
    usage_guest double,
    usage_guest_nice double,
    PRIMARY KEY ((hostname, bucket_day), timestamp_ns)
) WITH CLUSTERING ORDER BY (timestamp_ns ASC);
