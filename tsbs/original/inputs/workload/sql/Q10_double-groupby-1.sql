SELECT c.hostname, TIME_BUCKET('1 hour', c.event_ts) AS bucket,
       AVG(c.usage_user) AS avg_usage_user
FROM CPUReading c
WHERE c.event_ts >= :start_ts AND c.event_ts < :end_ts
GROUP BY c.hostname, TIME_BUCKET('1 hour', c.event_ts)
ORDER BY bucket ASC, c.hostname ASC;
