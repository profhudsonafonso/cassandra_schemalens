SELECT TIME_BUCKET('1 minute', c.event_ts) AS bucket,
       MAX(c.usage_user) AS max_usage_user
FROM CPUReading c
WHERE c.event_ts < :end_ts
GROUP BY TIME_BUCKET('1 minute', c.event_ts)
ORDER BY bucket DESC
LIMIT 5;
