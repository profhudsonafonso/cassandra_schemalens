-- SchemaLens TPCx-IoT Stage 4 (RESEARCH): DDL ONLY, NOT BENCHMARKED
-- ts0_series_raw: TS0; read and write routing unverified
-- M3 activated (potential compaction), no strategy automatically chosen
CREATE TABLE IF NOT EXISTS ts0_series_raw (
    driver_system_id text,
    sensor_name text,
    event_ts bigint,
    event_id uuid,
    sensor_reading double,
    padding text,
    PRIMARY KEY ((driver_system_id, sensor_name), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts ASC, event_id ASC);
