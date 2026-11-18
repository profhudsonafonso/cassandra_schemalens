-- Experimental Stage 4 CassandraCandidateSpec (schema only)
-- Family: TS6; variant: hour_host_dimension
-- ReadPlan: NOT VERIFIED; WritePlan: NOT IMPLEMENTED
CREATE TABLE IF NOT EXISTS ts6_cpu_by_hour_host (
    bucket_day date,
    hour_ns bigint,
    hostname text,
    sum_usage_user double,
    count_usage_user bigint,
    sum_usage_system double,
    count_usage_system bigint,
    sum_usage_idle double,
    count_usage_idle bigint,
    sum_usage_nice double,
    count_usage_nice bigint,
    sum_usage_iowait double,
    count_usage_iowait bigint,
    sum_usage_irq double,
    count_usage_irq bigint,
    sum_usage_softirq double,
    count_usage_softirq bigint,
    sum_usage_steal double,
    count_usage_steal bigint,
    sum_usage_guest double,
    count_usage_guest bigint,
    sum_usage_guest_nice double,
    count_usage_guest_nice bigint,
    PRIMARY KEY ((bucket_day, hour_ns), hostname)
) WITH CLUSTERING ORDER BY (hostname ASC);
