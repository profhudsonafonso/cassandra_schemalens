SELECT MIN(CASE WHEN r.event_ts BETWEEN :ti1_start AND :ti1_end THEN r.sensor_reading END) AS recent_stat,
       MIN(CASE WHEN r.event_ts BETWEEN :ti2_start AND :ti2_end THEN r.sensor_reading END) AS historical_stat
FROM Reading r
WHERE r.driver_system_id = :station_id AND r.sensor_name = :sensor_name
  AND ((r.event_ts BETWEEN :ti1_start AND :ti1_end)
    OR (r.event_ts BETWEEN :ti2_start AND :ti2_end));
