-- Blueprints only: do not treat this as a benchmark-ready deployment.
CREATE TABLE IF NOT EXISTS ts0_iot_raw (device_id text, sensor_name text, timestamp_ms bigint, value_double double, PRIMARY KEY ((device_id,sensor_name),timestamp_ms)) WITH CLUSTERING ORDER BY (timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS ts1_iot_bucket (device_id text, sensor_name text, bucket_day date, timestamp_ms bigint, value_double double, PRIMARY KEY ((device_id,sensor_name,bucket_day),timestamp_ms)) WITH CLUSTERING ORDER BY (timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS ts2_iot_wide (device_id text, bucket_day date, timestamp_ms bigint, m_s0 double, m_s1 double, PRIMARY KEY ((device_id,bucket_day),timestamp_ms)) WITH CLUSTERING ORDER BY (timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS ts4_iot_latest (device_id text, sensor_name text, latest_timestamp_ms bigint, value_double double, PRIMARY KEY ((device_id,sensor_name)));

CREATE TABLE IF NOT EXISTS ts5_iot_rollup (device_id text, sensor_name text, bucket_day date, window_start_ms bigint, value_sum double, sample_count bigint, minimum double, maximum double, PRIMARY KEY ((device_id,sensor_name,bucket_day),window_start_ms)) WITH CLUSTERING ORDER BY (window_start_ms DESC);
