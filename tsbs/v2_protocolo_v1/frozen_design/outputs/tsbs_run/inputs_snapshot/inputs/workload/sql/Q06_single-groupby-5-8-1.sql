SELECT TIME_BUCKET('1 minute', c.event_ts) AS bucket,
       MAX(c.usage_user) AS max_usage_user,
       MAX(c.usage_system) AS max_usage_system,
       MAX(c.usage_idle) AS max_usage_idle,
       MAX(c.usage_nice) AS max_usage_nice,
       MAX(c.usage_iowait) AS max_usage_iowait
FROM CPUReading c
WHERE c.hostname IN (:host_1, :host_2, :host_3, :host_4, :host_5, :host_6, :host_7, :host_8) AND
      c.event_ts >= :start_ts AND c.event_ts < :end_ts
GROUP BY TIME_BUCKET('1 minute', c.event_ts)
ORDER BY bucket ASC;
