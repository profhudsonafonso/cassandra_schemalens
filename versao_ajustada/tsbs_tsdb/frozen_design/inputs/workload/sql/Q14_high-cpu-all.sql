SELECT c.hostname, c.event_ts, c.usage_user, c.usage_system, c.usage_idle, c.usage_nice, c.usage_iowait, c.usage_irq, c.usage_softirq, c.usage_steal, c.usage_guest, c.usage_guest_nice
FROM CPUReading c
WHERE c.event_ts >= :start_ts AND c.event_ts < :end_ts AND
      c.usage_user > 90.0
ORDER BY c.event_ts ASC;
