-- SchemaLens TPCx-IoT Stage 4 (RESEARCH): DDL ONLY, NOT BENCHMARKED
-- ts3_sensor_3600s: TS3; read and write routing unverified
-- M3 activated (potential compaction), no strategy automatically chosen
CREATE TABLE IF NOT EXISTS ts3_sensor_3600s (
    driver_system_id text,
    sensor_name text,
    event_ts bigint,
    event_id uuid,
    sensor_reading double,
    padding text,
    bucket_start_ms bigint,
    PRIMARY KEY ((sensor_name, bucket_start_ms), event_ts, driver_system_id, event_id)
) WITH CLUSTERING ORDER BY (event_ts ASC, driver_system_id ASC, event_id ASC);
