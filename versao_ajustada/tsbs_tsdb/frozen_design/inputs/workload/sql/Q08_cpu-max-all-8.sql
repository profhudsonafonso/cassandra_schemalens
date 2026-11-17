SELECT TIME_BUCKET('1 hour', c.event_ts) AS bucket,
       MAX(c.usage_user) AS max_usage_user,
       MAX(c.usage_system) AS max_usage_system,
       MAX(c.usage_idle) AS max_usage_idle,
       MAX(c.usage_nice) AS max_usage_nice,
       MAX(c.usage_iowait) AS max_usage_iowait,
       MAX(c.usage_irq) AS max_usage_irq,
       MAX(c.usage_softirq) AS max_usage_softirq,
       MAX(c.usage_steal) AS max_usage_steal,
       MAX(c.usage_guest) AS max_usage_guest,
       MAX(c.usage_guest_nice) AS max_usage_guest_nice
FROM CPUReading c
WHERE c.hostname IN (:host_1, :host_2, :host_3, :host_4, :host_5, :host_6, :host_7, :host_8) AND
      c.event_ts >= :start_ts AND c.event_ts < :end_ts
GROUP BY TIME_BUCKET('1 hour', c.event_ts)
ORDER BY bucket ASC;
