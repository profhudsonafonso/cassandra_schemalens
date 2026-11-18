SELECT r.event_ts, r.sensor_reading
FROM Reading r
WHERE r.sensor_name = :sensor_name
  AND ((r.event_ts BETWEEN :ti1_start AND :ti1_end)
    OR (r.event_ts BETWEEN :ti2_start AND :ti2_end));
