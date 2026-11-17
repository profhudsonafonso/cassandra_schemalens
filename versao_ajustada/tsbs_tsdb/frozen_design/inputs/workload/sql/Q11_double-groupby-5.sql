SELECT c.hostname, TIME_BUCKET('1 hour', c.event_ts) AS bucket,
       AVG(c.usage_user) AS avg_usage_user,
       AVG(c.usage_system) AS avg_usage_system,
       AVG(c.usage_idle) AS avg_usage_idle,
       AVG(c.usage_nice) AS avg_usage_nice,
       AVG(c.usage_iowait) AS avg_usage_iowait
FROM CPUReading c
WHERE c.event_ts >= :start_ts AND c.event_ts < :end_ts
GROUP BY c.hostname, TIME_BUCKET('1 hour', c.event_ts)
ORDER BY bucket ASC, c.hostname ASC;
